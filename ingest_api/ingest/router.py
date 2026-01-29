"""Router central que clasifica y enruta lecturas a los pipelines correspondientes.

Este módulo implementa la lógica central de clasificación y enrutamiento.
Cada lectura se clasifica UNA VEZ y se enruta a exactamente UN pipeline.

FIX 2026-01-16: Ahora usa el SP sp_insert_reading_and_check_threshold para TODA
la lógica de inserción, evaluación de umbrales, alertas y notificaciones.
Esto garantiza consistencia con el backend NestJS y la creación correcta de
notificaciones en alert_notifications.
"""

from __future__ import annotations
import logging
import time

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy.orm import Session
from sqlalchemy import text

from iot_machine_learning.ml_service.reading_broker import ReadingBroker

from .common.validation import is_suspicious_zero_reading, log_suspicious_reading
from .common.guards import guard_reading, ValidationResult


class PipelineType(Enum):
    """Tipo de pipeline al que pertenece una lectura."""

    ALERT = "alert"
    WARNING = "warning"
    PREDICTION = "prediction"
    SP_HANDLED = "sp_handled"  # Nueva: manejado por el SP


class ReadingRouter:
    """Router central que clasifica y enruta lecturas a los pipelines.
    
    FIX: Ahora delega TODA la lógica al SP sp_insert_reading_and_check_threshold
    que maneja inserción, umbrales, alertas, notificaciones y delta detector.
    """

    def __init__(self, db: Session, broker: ReadingBroker) -> None:
        self._db = db
        self._broker = broker
        self._logger = logging.getLogger(__name__)
        self._sensor_type_cache: dict[int, str] = {}

    def _get_sensor_type(self, sensor_id: int) -> str:
        """Obtiene el tipo de sensor desde cache o BD."""
        if sensor_id in self._sensor_type_cache:
            return self._sensor_type_cache[sensor_id]
        
        try:
            result = self._db.execute(
                text("SELECT sensor_type FROM dbo.sensors WHERE id = :id"),
                {"id": sensor_id}
            ).fetchone()
            sensor_type = result[0] if result and result[0] else "unknown"
            self._sensor_type_cache[sensor_id] = sensor_type
            return sensor_type
        except Exception:
            return "unknown"

    def classify_and_route(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: datetime | None = None,
        ingest_timestamp: datetime | None = None,
        sensor_ts: float | None = None,      # PASO 0: Unix epoch preciso del sensor
        ingested_ts: float | None = None,    # PASO 0: Unix epoch cuando llegó a ingesta
        sequence: int | None = None,         # PASO 0: Número de secuencia
    ) -> PipelineType:
        """Procesa una lectura usando el SP centralizado.

        FIX 2026-01-16: Toda la lógica de clasificación, inserción, evaluación de
        umbrales, alertas, notificaciones y delta detector está ahora en el SP
        sp_insert_reading_and_check_threshold. Esto garantiza:
        - Consistencia con el backend NestJS
        - Creación correcta de notificaciones en alert_notifications
        - Evaluación de AMBOS umbrales (warning y critical)
        - Cooldown efectivo para delta spikes

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            device_timestamp: Timestamp del dispositivo (opcional)
            ingest_timestamp: Timestamp de ingesta (opcional, default: ahora)

        Returns:
            PipelineType.SP_HANDLED siempre (el SP maneja todo)
        """
        if ingest_timestamp is None:
            ingest_timestamp = datetime.now(timezone.utc)
        
        # PASO 0: Capturar timing si no viene
        if ingested_ts is None:
            ingested_ts = time.time()

        # Guard rail - validación temprana antes de cualquier procesamiento
        guard_result = guard_reading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            sensor_type=None,
        )
        if not guard_result.is_valid:
            self._logger.warning(
                "INGEST REJECTED sensor_id=%s value=%s reason=%s details=%s",
                sensor_id, value, guard_result.reason, guard_result.details,
            )
            return PipelineType.SP_HANDLED

        # Detectar lecturas sospechosas con valor 0.00000
        if is_suspicious_zero_reading(value):
            log_suspicious_reading(sensor_id, value, reason="exact_zero_value")

        # FIX: Usar el SP para TODA la lógica
        # El SP maneja: inserción, umbrales, alertas, notificaciones, delta detector
        try:
            self._db.execute(
                text(
                    """
                    EXEC sp_insert_reading_and_check_threshold 
                        @p_sensor_id = :sensor_id, 
                        @p_value = :value,
                        @p_device_timestamp = :device_ts
                    """
                ),
                {
                    "sensor_id": sensor_id,
                    "value": float(value),
                    "device_ts": device_timestamp,
                },
            )
            self._logger.info(
                "INGEST SP_HANDLED sensor_id=%s value=%s ts=%s",
                sensor_id,
                value,
                ingest_timestamp.isoformat(),
            )
        except Exception as e:
            self._logger.exception(
                "INGEST ERROR SP sensor_id=%s value=%s ts=%s err=%s",
                sensor_id,
                value,
                ingest_timestamp.isoformat(),
                type(e).__name__,
            )
            raise

        # Publicar al broker para ML (si está configurado)
        # El SP ya insertó la lectura, solo notificamos al broker
        try:
            if self._broker:
                from iot_machine_learning.ml_service.reading_broker import Reading
                # FIX: Reading requires sensor_type (str) and timestamp (float, not datetime)
                # Obtener sensor_type de la BD si es necesario
                sensor_type = self._get_sensor_type(sensor_id)
                
                # PASO 0: Usar sensor_ts preciso si está disponible
                reading_ts = sensor_ts if sensor_ts is not None else ingest_timestamp.timestamp()
                
                reading = Reading(
                    sensor_id=sensor_id,
                    sensor_type=sensor_type,
                    value=float(value),
                    timestamp=reading_ts,
                )
                self._broker.publish(reading)
                
                # PASO 0: Log de timing para verificación
                if sensor_ts is not None and ingested_ts is not None:
                    latency_ms = (ingested_ts - sensor_ts) * 1000
                    self._logger.debug(
                        "BROKER PUBLISHED sensor_id=%s value=%s sensor_ts=%.6f ingested_ts=%.6f latency_ms=%.2f seq=%s",
                        sensor_id, value, sensor_ts, ingested_ts, latency_ms, sequence
                    )
                else:
                    self._logger.debug(
                        "BROKER PUBLISHED sensor_id=%s value=%s ts=%.6f",
                        sensor_id, value, reading_ts
                    )
        except Exception as e:
            # No fallar si el broker falla - la lectura ya está persistida
            self._logger.warning(
                "BROKER PUBLISH FAILED sensor_id=%s err=%s: %s",
                sensor_id,
                type(e).__name__,
                str(e),
            )

        return PipelineType.SP_HANDLED


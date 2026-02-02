"""Router central que clasifica y enruta lecturas a los pipelines correspondientes.

Este módulo implementa la lógica central de clasificación y enrutamiento.
Cada lectura se clasifica UNA VEZ y se enruta a exactamente UN pipeline.

FIX 2026-01-16: Ahora usa el SP sp_insert_reading_and_check_threshold para TODA
la lógica de inserción, evaluación de umbrales, alertas y notificaciones.

FIX 2026-02-02: Agregado retry con backoff, deduplicación y DLQ.
"""

from __future__ import annotations
import logging
import time
from typing import Optional

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy.orm import Session
from sqlalchemy import text
from sqlalchemy.exc import OperationalError, DBAPIError

from iot_machine_learning.ml_service.reading_broker import ReadingBroker, Reading

from .common.validation import is_suspicious_zero_reading, log_suspicious_reading
from .common.guards import guard_reading, ValidationResult
from .resilience import MessageDeduplicator, DeadLetterQueue, retry_with_backoff, RetryConfig


class PipelineType(Enum):
    """Tipo de pipeline al que pertenece una lectura."""

    ALERT = "alert"
    WARNING = "warning"
    PREDICTION = "prediction"
    SP_HANDLED = "sp_handled"  # Nueva: manejado por el SP


# Configuración de retry para el SP
SP_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=5.0,
    retryable_exceptions=(OperationalError, DBAPIError),
)


class ReadingRouter:
    """Router central que clasifica y enruta lecturas a los pipelines.
    
    FIX: Ahora delega TODA la lógica al SP sp_insert_reading_and_check_threshold
    que maneja inserción, umbrales, alertas, notificaciones y delta detector.
    
    FIX 2026-02-02: Agregado:
    - Retry con backoff exponencial para el SP
    - Deduplicación de mensajes con Redis
    - Dead Letter Queue para mensajes fallidos
    """

    def __init__(
        self, 
        db: Session, 
        broker: ReadingBroker,
        deduplicator: Optional[MessageDeduplicator] = None,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        self._db = db
        self._broker = broker
        self._dedup = deduplicator
        self._dlq = dlq
        self._logger = logging.getLogger(__name__)
        self._sensor_type_cache: dict[int, str] = {}
        
        # Stats
        self._total_processed = 0
        self._total_duplicates = 0
        self._total_errors = 0

    @property
    def stats(self) -> dict:
        """Estadísticas del router."""
        return {
            "total_processed": self._total_processed,
            "total_duplicates": self._total_duplicates,
            "total_errors": self._total_errors,
            "dedup": self._dedup.stats if self._dedup else None,
            "dlq": self._dlq.stats if self._dlq else None,
        }

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
    
    def _execute_sp_with_retry(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime],
    ) -> None:
        """Ejecuta el SP con retry y backoff exponencial."""
        last_error: Optional[Exception] = None
        
        for attempt in range(1, SP_RETRY_CONFIG.max_attempts + 1):
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
                return  # Éxito
                
            except SP_RETRY_CONFIG.retryable_exceptions as e:
                last_error = e
                
                if attempt == SP_RETRY_CONFIG.max_attempts:
                    self._logger.error(
                        "SP_RETRY_EXHAUSTED sensor_id=%s attempts=%d err=%s",
                        sensor_id, attempt, e
                    )
                    raise
                
                delay = SP_RETRY_CONFIG.calculate_delay(attempt)
                self._logger.warning(
                    "SP_RETRY sensor_id=%s attempt=%d/%d delay=%.2fs err=%s",
                    sensor_id, attempt, SP_RETRY_CONFIG.max_attempts, delay, e
                )
                time.sleep(delay)
        
        if last_error:
            raise last_error

    def classify_and_route(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: datetime | None = None,
        ingest_timestamp: datetime | None = None,
        sensor_ts: float | None = None,      # PASO 0: Unix epoch preciso del sensor
        ingested_ts: float | None = None,    # PASO 0: Unix epoch cuando llegó a ingesta
        sequence: int | None = None,         # PASO 0: Número de secuencia
        msg_id: str | None = None,           # ID único para deduplicación
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
            # Enviar a DLQ para análisis
            if self._dlq:
                self._dlq.send(
                    payload={"sensor_id": sensor_id, "value": value},
                    error=guard_result.reason,
                    error_type="validation_error",
                    source="router",
                    sensor_id=sensor_id,
                )
            return PipelineType.SP_HANDLED

        # Deduplicación: verificar si ya procesamos este mensaje
        msg_id = msg_id or f"{sensor_id}:{sensor_ts or ingested_ts:.6f}:{value:.6f}"
        if self._dedup and self._dedup.is_duplicate(msg_id):
            self._total_duplicates += 1
            self._logger.debug(
                "INGEST DUPLICATE sensor_id=%s msg_id=%s",
                sensor_id, msg_id
            )
            return PipelineType.SP_HANDLED

        # Detectar lecturas sospechosas con valor 0.00000
        if is_suspicious_zero_reading(value):
            log_suspicious_reading(sensor_id, value, reason="exact_zero_value")

        # Ejecutar SP con retry
        try:
            self._execute_sp_with_retry(sensor_id, value, device_timestamp)
            self._total_processed += 1
            self._logger.info(
                "INGEST SP_HANDLED sensor_id=%s value=%s ts=%s",
                sensor_id,
                value,
                ingest_timestamp.isoformat(),
            )
        except Exception as e:
            self._total_errors += 1
            self._logger.exception(
                "INGEST ERROR SP sensor_id=%s value=%s ts=%s err=%s",
                sensor_id,
                value,
                ingest_timestamp.isoformat(),
                type(e).__name__,
            )
            # Enviar a DLQ
            if self._dlq:
                self._dlq.send(
                    payload={"sensor_id": sensor_id, "value": value, "device_ts": str(device_timestamp)},
                    error=str(e),
                    error_type="sp_error",
                    source="router",
                    sensor_id=sensor_id,
                    msg_id=msg_id,
                )
            raise

        # Publicar al broker para ML (si está configurado)
        self._publish_to_broker(
            sensor_id=sensor_id,
            value=value,
            sensor_ts=sensor_ts,
            ingested_ts=ingested_ts,
            ingest_timestamp=ingest_timestamp,
            sequence=sequence,
        )

        return PipelineType.SP_HANDLED
    
    def _publish_to_broker(
        self,
        sensor_id: int,
        value: float,
        sensor_ts: Optional[float],
        ingested_ts: Optional[float],
        ingest_timestamp: datetime,
        sequence: Optional[int],
    ) -> None:
        """Publica lectura al broker ML."""
        if not self._broker:
            return
        
        try:
            sensor_type = self._get_sensor_type(sensor_id)
            reading_ts = sensor_ts if sensor_ts is not None else ingest_timestamp.timestamp()
            
            reading = Reading(
                sensor_id=sensor_id,
                sensor_type=sensor_type,
                value=float(value),
                timestamp=reading_ts,
            )
            self._broker.publish(reading)
            
            if sensor_ts is not None and ingested_ts is not None:
                latency_ms = (ingested_ts - sensor_ts) * 1000
                self._logger.debug(
                    "BROKER PUBLISHED sensor_id=%s value=%s latency_ms=%.2f seq=%s",
                    sensor_id, value, latency_ms, sequence
                )
        except Exception as e:
            self._logger.warning(
                "BROKER PUBLISH FAILED sensor_id=%s err=%s: %s",
                sensor_id,
                type(e).__name__,
                str(e),
            )


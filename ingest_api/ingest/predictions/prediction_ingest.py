"""Pipeline de ingesta para PREDICCIONES.

Este pipeline procesa exclusivamente datos limpios para ML.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from iot_ingest_services.ml_service.reading_broker import ReadingBroker

from .prediction_rules import PredictionRules
from .prediction_dispatch import (
    dispatch_to_ml_broker,
    should_skip_prediction,
    update_latest_reading,
)


class PredictionIngestPipeline:
    """Pipeline de ingesta para predicciones ML."""

    def __init__(self, db: Session, broker: ReadingBroker) -> None:
        self._db = db
        self._broker = broker
        self._logger = logging.getLogger(__name__)

    def ingest(
        self,
        sensor_id: int,
        value: float,
        ingest_timestamp: datetime,
        device_timestamp: datetime | None = None,
    ) -> None:
        """Procesa una lectura en el pipeline de PREDICCIONES.

        Este método es DEFENSIVO: rechaza datos que no pertenecen a este pipeline.

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura (con decimales completos)
            ingest_timestamp: Timestamp de ingesta
            device_timestamp: Timestamp del dispositivo (opcional, no usado actualmente)

        Raises:
            ValueError: Si los datos no pertenecen a este pipeline
        """
        # Validación defensiva: rechazar si hay violación física o delta spike
        should_accept, reason = PredictionRules.accepts(
            self._db, sensor_id, value, ingest_timestamp
        )

        if not should_accept:
            raise ValueError(
                f"Prediction pipeline rechazó datos: {reason}. "
                f"Los datos deben estar limpios (sin violación física ni delta spike)."
            )

        # Regla PREDICTION (dedupe): comparar contra sensor_readings_latest.
        # - Igual EXACTO (comparación robusta) => NO persistir (ni latest) y NO enviar al ML.
        # - Cambio (aunque sea mínimo) => actualizar latest y enviar al ML.
        skip, reason = should_skip_prediction(self._db, sensor_id=sensor_id, value=value)
        if skip:
            self._logger.info(
                "INGEST SKIP PREDICTION sensor_id=%s value=%s ts=%s reason=%s",
                sensor_id,
                value,
                ingest_timestamp.isoformat(),
                reason,
            )
            return

        if PredictionRules.should_update_latest():
            update_latest_reading(
                db=self._db,
                sensor_id=sensor_id,
                value=value,
                ingest_timestamp=ingest_timestamp,
            )

        self._logger.info(
            "INGEST PERSIST PREDICTION sensor_id=%s value=%s ts=%s reason=%s",
            sensor_id,
            value,
            ingest_timestamp.isoformat(),
            reason,
        )

        if PredictionRules.should_forward_to_ml():
            dispatch_to_ml_broker(
                broker=self._broker,
                sensor_id=sensor_id,
                value=value,
                ingest_timestamp=ingest_timestamp,
            )


"""Pipeline de ingesta para WARNINGS.

Este pipeline procesa exclusivamente lecturas con delta spikes o cambios bruscos.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from .warning_rules import WarningRules
from .warning_persistence import persist_warning

# FIX Issue 5: Logger para depuración de delta spikes
logger = logging.getLogger(__name__)


class WarningIngestPipeline:
    """Pipeline de ingesta para advertencias de delta."""

    def __init__(self, db: Session) -> None:
        self._db = db

    def ingest(
        self,
        sensor_id: int,
        value: float,
        ingest_timestamp: datetime,
        device_timestamp: datetime | None = None,
    ) -> None:
        """Procesa una lectura en el pipeline de WARNINGS.

        Este método es DEFENSIVO: rechaza datos que no pertenecen a este pipeline.

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            ingest_timestamp: Timestamp de ingesta
            device_timestamp: Timestamp del dispositivo (opcional)

        Raises:
            ValueError: Si los datos no pertenecen a este pipeline
        """
        logger.debug(
            "[WARNING_INGEST] Processing sensor_id=%s value=%s ts=%s",
            sensor_id, value, ingest_timestamp
        )
        
        # Validación defensiva: rechazar si no hay delta spike
        should_accept, delta_info, reason = WarningRules.accepts(
            self._db, sensor_id, value, ingest_timestamp
        )

        if not should_accept or not delta_info:
            logger.debug(
                "[WARNING_INGEST] Rejected sensor_id=%s: %s",
                sensor_id, reason
            )
            raise ValueError(
                f"Warning pipeline rechazó datos: {reason}. "
                f"Los datos deben tener un delta spike detectado."
            )

        logger.info(
            "[WARNING_INGEST] DELTA SPIKE DETECTED sensor_id=%s value=%s delta_abs=%s",
            sensor_id, value,
            delta_info.get('delta_abs') if delta_info else None
        )

        # Persistir según las reglas del pipeline
        if WarningRules.should_persist_reading():
            persist_warning(
                db=self._db,
                sensor_id=sensor_id,
                value=value,
                delta_info=delta_info,
                ingest_timestamp=ingest_timestamp,
                device_timestamp=device_timestamp,
            )
            logger.info(
                "[WARNING_INGEST] Warning persisted sensor_id=%s value=%s",
                sensor_id, value
            )

        # Regla estricta: WARNING pipeline NUNCA envía datos al ML
        assert not WarningRules.should_forward_to_ml(), "WARNING pipeline nunca debe enviar a ML"


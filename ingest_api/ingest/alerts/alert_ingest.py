"""Pipeline de ingesta para ALERTAS.

Este pipeline procesa exclusivamente lecturas que violan el rango físico del sensor.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from .alert_rules import AlertRules
from .alert_persistence import persist_alert
from ..common.physical_ranges import PhysicalRange


class AlertIngestPipeline:
    """Pipeline de ingesta para alertas físicas."""

    def __init__(self, db: Session) -> None:
        self._db = db

    def ingest(
        self,
        sensor_id: int,
        value: float,
        ingest_timestamp: datetime,
        device_timestamp: datetime | None = None,
    ) -> None:
        """Procesa una lectura en el pipeline de ALERTAS.

        Este método es DEFENSIVO: rechaza datos que no pertenecen a este pipeline.

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            ingest_timestamp: Timestamp de ingesta
            device_timestamp: Timestamp del dispositivo (opcional)

        Raises:
            ValueError: Si los datos no pertenecen a este pipeline
        """
        # Validación defensiva: rechazar si no es una violación física
        should_accept, physical_range, reason = AlertRules.accepts(self._db, sensor_id, value)

        if not should_accept or not physical_range:
            raise ValueError(
                f"Alert pipeline rechazó datos: {reason}. "
                f"Los datos deben violar el rango físico del sensor."
            )

        # Persistir según las reglas del pipeline
        if AlertRules.should_persist_reading():
            persist_alert(
                db=self._db,
                sensor_id=sensor_id,
                value=value,
                physical_range=physical_range,
                ingest_timestamp=ingest_timestamp,
                device_timestamp=device_timestamp,
            )

        # Regla estricta: ALERT pipeline NUNCA envía datos al ML
        assert not AlertRules.should_forward_to_ml(), "ALERT pipeline nunca debe enviar a ML"


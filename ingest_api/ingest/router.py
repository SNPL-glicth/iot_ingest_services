"""Router central que clasifica y enruta lecturas a los pipelines correspondientes.

Este módulo implementa la lógica central de clasificación y enrutamiento.
Cada lectura se clasifica UNA VEZ y se enruta a exactamente UN pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from sqlalchemy.orm import Session

from iot_ingest_services.ml_service.reading_broker import ReadingBroker

from .common.physical_ranges import get_physical_range
from .common.delta_utils import get_delta_threshold, get_last_reading, check_delta_spike
from .alerts.alert_ingest import AlertIngestPipeline
from .warnings.warning_ingest import WarningIngestPipeline
from .predictions.prediction_ingest import PredictionIngestPipeline


class PipelineType(Enum):
    """Tipo de pipeline al que pertenece una lectura."""

    ALERT = "alert"
    WARNING = "warning"
    PREDICTION = "prediction"


class ReadingRouter:
    """Router central que clasifica y enruta lecturas a los pipelines."""

    def __init__(self, db: Session, broker: ReadingBroker) -> None:
        self._db = db
        self._broker = broker
        self._alert_pipeline = AlertIngestPipeline(db)
        self._warning_pipeline = WarningIngestPipeline(db)
        self._prediction_pipeline = PredictionIngestPipeline(db, broker)

    def classify_and_route(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: datetime | None = None,
        ingest_timestamp: datetime | None = None,
    ) -> PipelineType:
        """Clasifica una lectura y la enruta al pipeline correspondiente.

        Orden de evaluación (estricto):
        1. Verificar violación de rango físico → ALERT
        2. Verificar delta spike → WARNING
        3. Resto → PREDICTION (dato limpio)

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            device_timestamp: Timestamp del dispositivo (opcional)
            ingest_timestamp: Timestamp de ingesta (opcional, default: ahora)

        Returns:
            PipelineType al que fue enrutada la lectura
        """
        if ingest_timestamp is None:
            ingest_timestamp = datetime.now(timezone.utc)

        # 1. Verificar violación de rango físico (ALERT pipeline)
        physical_range = get_physical_range(self._db, sensor_id)
        if physical_range and physical_range.violates(value):
            self._alert_pipeline.ingest(
                sensor_id=sensor_id,
                value=value,
                ingest_timestamp=ingest_timestamp,
                device_timestamp=device_timestamp,
            )
            return PipelineType.ALERT

        # 2. Verificar delta spike (WARNING pipeline)
        last_reading = get_last_reading(self._db, sensor_id)
        if last_reading:
            delta_threshold = get_delta_threshold(self._db, sensor_id)
            if delta_threshold:
                delta_info = check_delta_spike(
                    current_value=value,
                    current_ts=ingest_timestamp,
                    last_reading=last_reading,
                    delta_threshold=delta_threshold,
                )
                if delta_info and delta_info.get("is_spike", False):
                    self._warning_pipeline.ingest(
                        sensor_id=sensor_id,
                        value=value,
                        ingest_timestamp=ingest_timestamp,
                        device_timestamp=device_timestamp,
                    )
                    return PipelineType.WARNING

        # 3. Dato limpio para ML (PREDICTION pipeline)
        self._prediction_pipeline.ingest(
            sensor_id=sensor_id,
            value=value,
            ingest_timestamp=ingest_timestamp,
            device_timestamp=device_timestamp,
        )
        return PipelineType.PREDICTION


"""Lógica de dispatch para el pipeline de PREDICCIONES."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from iot_ingest_services.ml_service.reading_broker import Reading, ReadingBroker


def update_latest_reading(
    db: Session,
    sensor_id: int,
    value: float,
    ingest_timestamp: datetime,
) -> None:
    """Actualiza sensor_readings_latest sin persistir la lectura completa.

    Regla: Solo mantener el último valor actualizado, no guardar historial masivo.
    """
    db.execute(
        text(
            """
            MERGE dbo.sensor_readings_latest AS tgt
            USING (
                SELECT :sensor_id AS sensor_id, :value AS latest_value, :ts AS latest_timestamp
            ) AS src
                ON tgt.sensor_id = src.sensor_id
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.latest_value = src.latest_value,
                    tgt.latest_timestamp = src.latest_timestamp
            WHEN NOT MATCHED THEN
                INSERT (sensor_id, latest_value, latest_timestamp)
                VALUES (src.sensor_id, src.latest_value, src.latest_timestamp);
            """
        ),
        {
            "sensor_id": sensor_id,
            "value": value,
            "ts": ingest_timestamp,
        },
    )


def dispatch_to_ml_broker(
    broker: ReadingBroker,
    sensor_id: int,
    value: float,
    ingest_timestamp: datetime,
) -> None:
    """Envía datos limpios al broker ML para procesamiento online.

    Regla: Solo datos limpios (sin violaciones físicas ni delta spikes) van al ML.
    Se conservan decimales completos para precisión en ML.
    """
    now_ts = ingest_timestamp.timestamp()
    reading = Reading(
        sensor_id=sensor_id,
        sensor_type="unknown",  # opcional: resolver desde BD si lo necesitas
        value=float(value),  # Conservar decimales completos
        timestamp=now_ts,
    )
    broker.publish(reading)


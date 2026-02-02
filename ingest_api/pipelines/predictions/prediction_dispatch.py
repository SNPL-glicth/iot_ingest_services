"""Lógica de dispatch para el pipeline de PREDICCIONES."""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from ...core.domain.broker_interface import IReadingBroker, Reading

# FIX AUDITORIA: Logger para diagnóstico de dedupe
_logger = logging.getLogger(__name__)


def _to_decimal(value: float) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def get_latest_value(db: Session, sensor_id: int) -> float | None:
    row = db.execute(
        text(
            """
            SELECT TOP 1 latest_value
            FROM dbo.sensor_readings_latest
            WHERE sensor_id = :sensor_id
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row:
        return None
    return float(row[0])


def should_skip_prediction(
    db: Session,
    sensor_id: int,
    value: float,
) -> tuple[bool, str]:
    """Determina si se debe omitir la predicción por dedupe.
    
    FIX AUDITORIA: Logging mejorado para diagnóstico de problema 3.
    """
    latest = get_latest_value(db, sensor_id)
    if latest is None:
        _logger.debug(
            "PREDICTION DEDUPE sensor_id=%s value=%s result=PROCESS reason=no_latest",
            sensor_id, value,
        )
        return False, "no_latest"

    a = _to_decimal(value)
    b = _to_decimal(latest)
    if a is None or b is None:
        _logger.warning(
            "PREDICTION DEDUPE sensor_id=%s value=%s latest=%s result=PROCESS reason=bad_decimal",
            sensor_id, value, latest,
        )
        return False, "bad_decimal"

    if a == b:
        _logger.debug(
            "PREDICTION DEDUPE sensor_id=%s value=%s latest=%s result=SKIP reason=same_as_latest",
            sensor_id, value, latest,
        )
        return True, "same_as_latest"

    _logger.debug(
        "PREDICTION DEDUPE sensor_id=%s value=%s latest=%s delta=%s result=PROCESS reason=changed",
        sensor_id, value, latest, float(a - b),
    )
    return False, "changed"


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


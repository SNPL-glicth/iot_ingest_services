"""Prediction insertion for batch runner."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text


def insert_prediction(
    conn,
    *,
    model_id: int,
    sensor_id: int,
    device_id: int,
    predicted_value: float,
    confidence: float,
    target_ts_utc: datetime,
) -> int:
    row = conn.execute(
        text(
            """
            INSERT INTO dbo.predictions (
              model_id, sensor_id, device_id, predicted_value, confidence,
              predicted_at, target_timestamp
            )
            OUTPUT INSERTED.id
            VALUES (
              :model_id, :sensor_id, :device_id, :predicted_value, :confidence,
              GETDATE(), :target_timestamp
            )
            """
        ),
        {
            "model_id": model_id,
            "sensor_id": sensor_id,
            "device_id": device_id,
            "predicted_value": predicted_value,
            "confidence": confidence,
            "target_timestamp": target_ts_utc.replace(tzinfo=None),
        },
    ).fetchone()

    if not row:
        raise RuntimeError("failed to insert prediction")
    return int(row[0])

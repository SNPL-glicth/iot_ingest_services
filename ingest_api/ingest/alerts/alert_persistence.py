"""Lógica de persistencia para el pipeline de ALERTAS."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..common.physical_ranges import PhysicalRange


def persist_alert(
    db: Session,
    sensor_id: int,
    value: float,
    physical_range: PhysicalRange,
    ingest_timestamp: datetime,
    device_timestamp: datetime | None = None,
) -> None:
    """Persiste una alerta física.

    Reglas de persistencia:
    - Actualiza sensor_readings_latest
    - Guarda la lectura que rompe el umbral
    - Cierra alertas activas previas del mismo sensor (1 alerta activa por sensor)
    - Crea nueva alerta activa con severity=critical
    """
    # 1. Insertar la lectura relevante actual (SIEMPRE)
    db.execute(
        text(
            """
            INSERT INTO dbo.sensor_readings (sensor_id, value, timestamp, device_timestamp)
            VALUES (:sensor_id, :value, :ts, :device_ts)
            """
        ),
        {
            "sensor_id": sensor_id,
            "value": value,
            "ts": ingest_timestamp,
            "device_ts": device_timestamp,
        },
    )

    # 2. Obtener device_id para la alerta
    device_row = db.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not device_row:
        return
    device_id = int(device_row[0])

    # 3. Mantener UNA alerta activa por sensor.
    #    Si ya existe una activa, se actualiza (timestamp/value/threshold/device).
    #    Si no existe, se crea.
    db.execute(
        text(
            """
            DECLARE @existing_id INT;

            SELECT TOP 1 @existing_id = id
            FROM dbo.alerts
            WHERE sensor_id = :sensor_id
              AND status = 'active'
            ORDER BY triggered_at DESC;

            IF @existing_id IS NULL
            BEGIN
                INSERT INTO dbo.alerts (
                    threshold_id,
                    sensor_id,
                    device_id,
                    severity,
                    status,
                    triggered_value,
                    triggered_at
                )
                VALUES (
                    :threshold_id,
                    :sensor_id,
                    :device_id,
                    'critical',
                    'active',
                    :value,
                    :ts
                );
            END
            ELSE
            BEGIN
                UPDATE dbo.alerts
                SET threshold_id = :threshold_id,
                    device_id = :device_id,
                    severity = 'critical',
                    triggered_value = :value,
                    triggered_at = :ts
                WHERE id = @existing_id;
            END
            """
        ),
        {
            "threshold_id": physical_range.threshold_id,
            "sensor_id": sensor_id,
            "device_id": device_id,
            "value": value,
            "ts": ingest_timestamp,
        },
    )


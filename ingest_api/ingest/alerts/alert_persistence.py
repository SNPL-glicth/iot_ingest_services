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
    # 1. Actualizar última lectura (siempre, para mantener estado actualizado)
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

    # 2. Guardar la lectura que rompe el umbral (evento relevante)
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

    # 3. Obtener device_id para la alerta
    device_row = db.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not device_row:
        return
    device_id = int(device_row[0])

    # 4. Cerrar alertas activas previas del mismo sensor (1 alerta activa por sensor)
    db.execute(
        text(
            """
            UPDATE dbo.alerts
            SET status = 'resolved',
                resolved_at = GETDATE()
            WHERE sensor_id = :sensor_id
              AND status = 'active'
            """
        ),
        {"sensor_id": sensor_id},
    )

    # 5. Crear nueva alerta activa (severity siempre = critical)
    db.execute(
        text(
            """
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
            )
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


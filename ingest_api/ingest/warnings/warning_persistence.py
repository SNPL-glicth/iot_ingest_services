"""Lógica de persistencia para el pipeline de WARNINGS."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


def persist_warning(
    db: Session,
    sensor_id: int,
    value: float,
    delta_info: dict,
    ingest_timestamp: datetime,
    device_timestamp: datetime | None = None,
) -> None:
    """Persiste una advertencia de delta spike.

    Reglas de persistencia:
    - Actualiza sensor_readings_latest
    - Guarda el evento de delta spike
    - Cierra advertencias activas previas del mismo sensor (1 advertencia activa por sensor)
    - Crea nuevo evento ML de tipo DELTA_SPIKE
    """
    # 1. Actualizar última lectura (mantener estado actualizado)
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

    # 2. Guardar el evento de delta spike (lectura relevante)
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

    # 3. Obtener device_id
    device_row = db.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not device_row:
        return
    device_id = int(device_row[0])

    # 4. Cerrar advertencias activas previas del mismo sensor (1 advertencia activa por sensor)
    db.execute(
        text(
            """
            UPDATE dbo.ml_events
            SET status = 'resolved',
                resolved_at = GETDATE()
            WHERE sensor_id = :sensor_id
              AND event_code = 'DELTA_SPIKE'
              AND status = 'active'
            """
        ),
        {"sensor_id": sensor_id},
    )

    # 5. Crear nuevo evento ML de tipo warning (DELTA_SPIKE)
    event_type = delta_info.get("severity", "warning")
    if event_type not in ("info", "warning", "critical"):
        event_type = "warning"

    payload = {
        "delta_abs": delta_info.get("delta_abs"),
        "delta_rel": delta_info.get("delta_rel"),
        "slope_abs": delta_info.get("slope_abs"),
        "slope_rel": delta_info.get("slope_rel"),
        "dt_seconds": delta_info.get("dt_seconds"),
        "last_value": delta_info.get("last_value"),
        "triggered_thresholds": delta_info.get("triggered_thresholds", []),
    }

    db.execute(
        text(
            """
            INSERT INTO dbo.ml_events (
                device_id,
                sensor_id,
                event_type,
                event_code,
                title,
                message,
                status,
                created_at,
                payload
            )
            VALUES (
                :device_id,
                :sensor_id,
                :event_type,
                'DELTA_SPIKE',
                :title,
                :message,
                'active',
                :ts,
                :payload
            )
            """
        ),
        {
            "device_id": device_id,
            "sensor_id": sensor_id,
            "event_type": event_type,
            "title": f"Delta spike detectado en sensor {sensor_id}",
            "message": delta_info.get("reason", ""),
            "ts": ingest_timestamp,
            "payload": json.dumps(payload, ensure_ascii=False),
        },
    )


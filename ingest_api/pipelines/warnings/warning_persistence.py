"""Lógica de persistencia para el pipeline de WARNINGS."""

from __future__ import annotations

import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


def _build_spike_title(delta_info: dict) -> str:
    """Genera un título explicativo para el evento de delta spike.
    
    FIX: Mensaje más claro para el usuario en lugar de título genérico.
    """
    severity = delta_info.get("severity", "warning")
    triggered = delta_info.get("triggered_thresholds", [])
    
    if severity == "critical":
        return "Cambio crítico detectado"
    elif "abs_delta" in triggered or "rel_delta" in triggered:
        return "Cambio significativo en valor"
    elif "abs_slope" in triggered or "rel_slope" in triggered:
        return "Cambio rápido detectado"
    else:
        return "Variación inusual detectada"


def _build_spike_message(delta_info: dict) -> str:
    """Genera un mensaje explicativo para el usuario.
    
    FIX: Traduce los datos técnicos a lenguaje comprensible.
    """
    parts = []
    
    delta_abs = delta_info.get("delta_abs")
    last_value = delta_info.get("last_value")
    triggered = delta_info.get("triggered_thresholds", [])
    
    if delta_abs is not None and last_value is not None:
        direction = "subió" if delta_info.get("delta_abs", 0) > 0 else "cambió"
        parts.append(f"El valor {direction} {delta_abs:.2f} unidades desde {last_value:.2f}")
    
    if "abs_slope" in triggered or "rel_slope" in triggered:
        dt = delta_info.get("dt_seconds", 0)
        if dt > 0:
            parts.append(f"en {dt:.0f} segundos")
    
    reason = delta_info.get("reason", "")
    if reason and not parts:
        return reason
    
    return ". ".join(parts) if parts else "Cambio detectado fuera del comportamiento normal."


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

    # 1. Insertar la lectura relevante actual (delta spike)
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

    # 2. Obtener device_id
    device_row = db.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not device_row:
        return
    device_id = int(device_row[0])

    # 3. Mantener UNA advertencia activa (ml_event DELTA_SPIKE) por sensor.
    #    Si ya existe una activa, se actualiza (timestamp/payload/message/event_type).
    #    Si no existe, se crea.
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
            DECLARE @existing_id INT;

            SELECT TOP 1 @existing_id = id
            FROM dbo.ml_events
            WHERE sensor_id = :sensor_id
              AND event_code = 'DELTA_SPIKE'
              AND status = 'active'
            ORDER BY created_at DESC;

            IF @existing_id IS NULL
            BEGIN
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
                );
            END
            ELSE
            BEGIN
                UPDATE dbo.ml_events
                SET device_id = :device_id,
                    event_type = :event_type,
                    title = :title,
                    message = :message,
                    created_at = :ts,
                    payload = :payload
                WHERE id = @existing_id;
            END
            """
        ),
        {
            "device_id": device_id,
            "sensor_id": sensor_id,
            "event_type": event_type,
            "title": _build_spike_title(delta_info),
            "message": _build_spike_message(delta_info),
            "ts": ingest_timestamp,
            "payload": json.dumps(payload, ensure_ascii=False),
        },
    )


"""Flujos de ingesta separados por propósito.

Este módulo implementa los 3 flujos de ingesta:

1. alert_ingest: Solo guarda última lectura válida + lectura que rompe umbral
2. warning_ingest: Solo guarda eventos de delta spike, no el stream completo
3. prediction_ingest: Guarda datos limpios para ML (ventanas cortas + agregados)

Reglas de persistencia:
- NO guardar todas las lecturas masivamente
- Guardar solo: último valor por sensor + eventos relevantes
- Evitar locks largos y transacciones pesadas
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from .classification import ClassifiedReading


def ingest_alert(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
) -> None:
    """Ingesta para ALERTAS (hard rules).

    Solo guarda:
    - Última lectura válida (en sensor_readings_latest)
    - Lectura que rompe el umbral (en sensor_readings + alert)

    Genera 1 alerta activa por sensor (no acumulable).
    Severity siempre = critical.
    """
    sensor_id = classified.sensor_id
    value = classified.value
    physical_range = classified.physical_range

    if not physical_range:
        # Sin rango físico, no podemos generar alerta
        return

    # Guardar la lectura que rompe el umbral (evento relevante)
    device_ts = classified.device_timestamp
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
            "device_ts": device_ts,
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


def ingest_warning(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
) -> None:
    """Ingesta para ADVERTENCIAS (delta/spike).

    Solo guarda eventos, no el stream completo.
    Máximo 1 advertencia activa por sensor.
    """
    sensor_id = classified.sensor_id
    value = classified.value
    delta_info = classified.delta_info

    if not delta_info or not delta_info.get("is_spike", False):
        return

    # Guardar el evento de delta spike (lectura relevante)
    device_ts = classified.device_timestamp
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
            "device_ts": device_ts,
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
            "message": classified.reason,
            "ts": ingest_timestamp,
            "payload": json.dumps(payload, ensure_ascii=False),
        },
    )


def ingest_prediction(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
) -> None:
    """Ingesta para ML/PREDICCIÓN (datos limpios).

    Solo entran lecturas:
    - Sin violación física
    - Sin delta spike fuerte

    Se conservan decimales.
    El ML NO recibe datos crudos masivos.
    Se trabajan ventanas cortas en memoria + agregados (1m, 5m, 1h).
    """
    sensor_id = classified.sensor_id
    value = classified.value

    # 1. Actualizar última lectura (siempre mantener estado actualizado)
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

    # DEV/QA mode: persist full stream for charts/debugging.
    # By default this remains disabled to avoid massive writes.
    persist_all = str(os.getenv("INGEST_PERSIST_ALL_READINGS", "false")).strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    if persist_all:
        device_ts = classified.device_timestamp
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
                "device_ts": device_ts,
            },
        )

    # 2. NO guardamos todas las lecturas aquí.
    # El ML online consume desde el broker en memoria (ventanas deslizantes).
    # Solo guardamos agregados periódicos si es necesario (implementación futura).
    # Por ahora, el broker en memoria es suficiente para ventanas cortas.

    # Nota: La lectura se publicará en el broker para ML online,
    # pero NO se persiste masivamente en sensor_readings.
    # Esto reduce la carga en la BD y evita deadlocks.


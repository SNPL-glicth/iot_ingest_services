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

    # 4. CRÍTICO: Crear notificación en alert_notifications
    #    Esto es lo que faltaba - las alertas físicas NO estaban creando notificaciones
    #    Solo se crea si es una alerta NUEVA (no update de existente)
    _create_alert_notification(
        db=db,
        sensor_id=sensor_id,
        device_id=device_id,
        value=value,
        physical_range=physical_range,
        ingest_timestamp=ingest_timestamp,
    )


def _create_alert_notification(
    db: Session,
    sensor_id: int,
    device_id: int,
    value: float,
    physical_range: PhysicalRange,
    ingest_timestamp: datetime,
) -> None:
    """Crea una notificación para una alerta física.
    
    SSOT: La tabla alert_notifications es la fuente de verdad para READ/UNREAD.
    
    Reglas:
    - source = 'alert' (NO 'ml_event')
    - severity = 'critical' (alertas físicas siempre son críticas)
    - Deduplicación: no crear si ya existe una notificación no leída para este sensor
    """
    # Obtener nombre del sensor para el título
    sensor_row = db.execute(
        text("SELECT name FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    sensor_name = sensor_row[0] if sensor_row else f"Sensor {sensor_id}"

    # Obtener nombre del dispositivo
    device_row = db.execute(
        text("SELECT name FROM dbo.devices WHERE id = :device_id"),
        {"device_id": device_id},
    ).fetchone()
    device_name = device_row[0] if device_row else f"Dispositivo {device_id}"

    # Deduplicación: verificar si ya existe una notificación no leída reciente
    # para este sensor (evita spam de notificaciones)
    existing = db.execute(
        text(
            """
            SELECT TOP 1 id FROM dbo.alert_notifications
            WHERE source = 'alert'
              AND is_read = 0
              AND source_event_id IN (
                  SELECT id FROM dbo.alerts 
                  WHERE sensor_id = :sensor_id AND status = 'active'
              )
              AND created_at >= DATEADD(minute, -5, GETDATE())
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if existing:
        # Ya existe una notificación reciente no leída, no crear duplicado
        return

    # Obtener el ID de la alerta recién creada/actualizada
    alert_row = db.execute(
        text(
            """
            SELECT TOP 1 id FROM dbo.alerts
            WHERE sensor_id = :sensor_id AND status = 'active'
            ORDER BY triggered_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not alert_row:
        return

    alert_id = int(alert_row[0])

    # Crear la notificación
    title = f"🚨 ALERTA CRÍTICA: {sensor_name}"
    message = (
        f"Valor {value:.2f} fuera de rango físico "
        f"[{physical_range.min_value:.2f} - {physical_range.max_value:.2f}] "
        f"en {device_name}"
    )

    db.execute(
        text(
            """
            INSERT INTO dbo.alert_notifications (
                source,
                source_event_id,
                severity,
                title,
                message,
                is_read,
                created_at
            )
            VALUES (
                'alert',
                :alert_id,
                'critical',
                :title,
                :message,
                0,
                :created_at
            )
            """
        ),
        {
            "alert_id": alert_id,
            "title": title,
            "message": message,
            "created_at": ingest_timestamp,
        },
    )


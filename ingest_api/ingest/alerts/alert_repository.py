"""Repositorio de alertas - operaciones de persistencia.

Módulo separado de alert_persistence.py para mantener archivos <180 líneas.
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..common.physical_ranges import PhysicalRange

logger = logging.getLogger(__name__)


def insert_reading(
    db: Session,
    sensor_id: int,
    value: float,
    ingest_timestamp: datetime,
    device_timestamp: datetime | None = None,
) -> None:
    """Inserta la lectura que disparó la alerta."""
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


def get_device_id(db: Session, sensor_id: int) -> int | None:
    """Obtiene el device_id para un sensor."""
    device_row = db.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    return int(device_row[0]) if device_row else None


def upsert_alert(
    db: Session,
    sensor_id: int,
    device_id: int,
    threshold_id: int,
    value: float,
    ingest_timestamp: datetime,
) -> None:
    """Crea o actualiza una alerta activa para el sensor.
    
    Mantiene UNA alerta activa por sensor.
    """
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
            "threshold_id": threshold_id,
            "sensor_id": sensor_id,
            "device_id": device_id,
            "value": value,
            "ts": ingest_timestamp,
        },
    )


def get_active_alert_id(db: Session, sensor_id: int) -> int | None:
    """Obtiene el ID de la alerta activa más reciente para un sensor."""
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
    return int(alert_row[0]) if alert_row else None


def get_sensor_name(db: Session, sensor_id: int) -> str:
    """Obtiene el nombre del sensor."""
    sensor_row = db.execute(
        text("SELECT name FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    return sensor_row[0] if sensor_row else f"Sensor {sensor_id}"


def get_device_name(db: Session, device_id: int) -> str:
    """Obtiene el nombre del dispositivo."""
    device_row = db.execute(
        text("SELECT name FROM dbo.devices WHERE id = :device_id"),
        {"device_id": device_id},
    ).fetchone()
    return device_row[0] if device_row else f"Dispositivo {device_id}"


def has_recent_unread_notification(db: Session, sensor_id: int) -> bool:
    """Verifica si existe una notificación no leída reciente para el sensor."""
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
    return existing is not None


def insert_notification(
    db: Session,
    alert_id: int,
    title: str,
    message: str,
    created_at: datetime,
) -> None:
    """Inserta una notificación de alerta."""
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
            "created_at": created_at,
        },
    )

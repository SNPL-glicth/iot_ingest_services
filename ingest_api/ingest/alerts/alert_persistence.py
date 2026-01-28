"""Lógica de persistencia para el pipeline de ALERTAS."""

from __future__ import annotations

import os
import logging
from datetime import datetime

import requests
from sqlalchemy import text
from sqlalchemy.orm import Session

from ..common.physical_ranges import PhysicalRange
from ...classification.sensor_state import SensorStateManager, SensorOperationalState

logger = logging.getLogger(__name__)


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

    # 4. SSOT: Actualizar estado operacional del sensor a ALERT
    #    Esto garantiza consistencia entre alerts table y sensors.operational_state
    try:
        state_manager = SensorStateManager(db)
        success, msg, is_new = state_manager.on_threshold_violated(
            sensor_id=sensor_id,
            severity="critical",  # Alertas físicas siempre son críticas
            reason=f"Physical threshold violation: {value}",
        )
        if success:
            logger.debug(f"[SSOT] Sensor {sensor_id} state updated: {msg}")
        else:
            logger.warning(f"[SSOT] Failed to update sensor {sensor_id} state: {msg}")
    except Exception as e:
        logger.error(f"[SSOT] Error updating sensor state: {e}")

    # 5. CRÍTICO: Crear notificación en alert_notifications
    #    Esto es lo que faltaba - las alertas físicas NO estaban creando notificaciones
    #    Solo se crea si es una alerta NUEVA (no update de existente)
    alert_id = _create_alert_notification(
        db=db,
        sensor_id=sensor_id,
        device_id=device_id,
        value=value,
        physical_range=physical_range,
        ingest_timestamp=ingest_timestamp,
    )
    
    # 6. PUSH NOTIFICATION: Disparar push via backend NestJS
    if alert_id:
        _trigger_push_notification(alert_id)


def _create_alert_notification(
    db: Session,
    sensor_id: int,
    device_id: int,
    value: float,
    physical_range: PhysicalRange,
    ingest_timestamp: datetime,
) -> int | None:
    """Crea una notificación para una alerta física.
    
    SSOT: La tabla alert_notifications es la fuente de verdad para READ/UNREAD.
    
    Reglas:
    - source = 'alert' (NO 'ml_event')
    - severity = 'critical' (alertas físicas siempre son críticas)
    - Deduplicación: no crear si ya existe una notificación no leída para este sensor
    
    Returns:
        alert_id if notification was created, None otherwise
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
        return None

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
        return None

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
    
    return alert_id


def _trigger_push_notification(alert_id: int) -> None:
    """Dispara push notification via backend NestJS.
    
    Llama al endpoint interno del backend para enviar FCM push.
    No bloquea si falla - solo loguea el error.
    """
    backend_url = os.getenv("BACKEND_URL", "http://localhost:3000")
    internal_key = os.getenv("INTERNAL_API_KEY")
    
    if not internal_key:
        logger.warning("[PUSH] INTERNAL_API_KEY not configured - skipping push trigger")
        return
    
    try:
        response = requests.post(
            f"{backend_url}/notifications/internal/trigger-push",
            json={
                "type": "alert",
                "alertId": str(alert_id),
            },
            headers={
                "X-Internal-Key": internal_key,
                "Content-Type": "application/json",
            },
            timeout=5,
        )
        
        if response.ok:
            logger.info(f"[PUSH] Alert push triggered for alertId={alert_id}")
        else:
            logger.warning(f"[PUSH] Failed to trigger push: {response.status_code} {response.text}")
    except Exception as e:
        logger.error(f"[PUSH] Error triggering push notification: {e}")


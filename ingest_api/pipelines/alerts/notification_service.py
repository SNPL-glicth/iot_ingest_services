"""Servicio de notificaciones para alertas.

Módulo separado de alert_persistence.py para mantener archivos <180 líneas.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime

import requests
from sqlalchemy.orm import Session

from ..shared.physical_ranges import PhysicalRange
from . import alert_repository as repo

logger = logging.getLogger(__name__)


def create_alert_notification(
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
    # Deduplicación
    if repo.has_recent_unread_notification(db, sensor_id):
        return None

    # Obtener el ID de la alerta
    alert_id = repo.get_active_alert_id(db, sensor_id)
    if not alert_id:
        return None

    # Obtener nombres para el mensaje
    sensor_name = repo.get_sensor_name(db, sensor_id)
    device_name = repo.get_device_name(db, device_id)

    # Crear la notificación
    title = f"🚨 ALERTA CRÍTICA: {sensor_name}"
    message = (
        f"Valor {value:.2f} fuera de rango físico "
        f"[{physical_range.min_value:.2f} - {physical_range.max_value:.2f}] "
        f"en {device_name}"
    )

    repo.insert_notification(
        db=db,
        alert_id=alert_id,
        title=title,
        message=message,
        created_at=ingest_timestamp,
    )
    
    return alert_id


def trigger_push_notification(alert_id: int) -> None:
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

"""Lógica de persistencia para el pipeline de ALERTAS.

Refactorizado 2026-02-02: Separado en módulos <180 líneas.
- alert_repository.py: Operaciones de BD
- notification_service.py: Notificaciones y push
"""

from __future__ import annotations

import logging
from datetime import datetime

from sqlalchemy.orm import Session

from ..shared.physical_ranges import PhysicalRange
from ...classification import SensorStateManager
from . import alert_repository as repo
from . import notification_service as notif

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
    - Guarda la lectura que rompe el umbral
    - Mantiene UNA alerta activa por sensor
    - Actualiza estado operacional del sensor
    - Crea notificación y dispara push
    """
    # 1. Insertar la lectura
    repo.insert_reading(db, sensor_id, value, ingest_timestamp, device_timestamp)

    # 2. Obtener device_id
    device_id = repo.get_device_id(db, sensor_id)
    if not device_id:
        return

    # 3. Upsert alerta
    repo.upsert_alert(
        db=db,
        sensor_id=sensor_id,
        device_id=device_id,
        threshold_id=physical_range.threshold_id,
        value=value,
        ingest_timestamp=ingest_timestamp,
    )

    # 4. Actualizar estado operacional del sensor
    _update_sensor_state(db, sensor_id, value)

    # 5. Crear notificación
    alert_id = notif.create_alert_notification(
        db=db,
        sensor_id=sensor_id,
        device_id=device_id,
        value=value,
        physical_range=physical_range,
        ingest_timestamp=ingest_timestamp,
    )
    
    # 6. Disparar push
    if alert_id:
        notif.trigger_push_notification(alert_id)


def _update_sensor_state(db: Session, sensor_id: int, value: float) -> None:
    """Actualiza el estado operacional del sensor a ALERT."""
    try:
        state_manager = SensorStateManager(db)
        success, msg, _ = state_manager.on_threshold_violated(
            sensor_id=sensor_id,
            severity="critical",
            reason=f"Physical threshold violation: {value}",
        )
        if success:
            logger.debug(f"[SSOT] Sensor {sensor_id} state updated: {msg}")
        else:
            logger.warning(f"[SSOT] Failed to update sensor {sensor_id} state: {msg}")
    except Exception as e:
        logger.error(f"[SSOT] Error updating sensor state: {e}")


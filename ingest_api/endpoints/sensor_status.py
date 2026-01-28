"""Endpoint para estado consolidado del sensor."""

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from iot_ingest_services.common.db import get_db
from ..auth import require_api_key
from ..queries import (
    get_active_alert,
    get_active_warning,
    get_current_prediction,
    compute_final_state,
)
from ..schemas import SensorConsolidatedStatus

router = APIRouter(tags=["sensors"])


@router.get(
    "/sensors/{sensor_id}/status",
    response_model=SensorConsolidatedStatus,
    dependencies=[Depends(require_api_key)],
)
def get_sensor_consolidated_status(
    sensor_id: int, 
    db: Session = Depends(get_db)
):
    """Obtiene el estado consolidado del sensor.
    
    Incluye alertas activas, warnings y predicciones.
    """
    alert_active = get_active_alert(db, sensor_id)
    warning_active = get_active_warning(db, sensor_id)
    prediction_current = get_current_prediction(db, sensor_id)

    final_state = compute_final_state(
        alert_active=alert_active,
        warning_active=warning_active,
        prediction_current=prediction_current,
    )

    return SensorConsolidatedStatus(
        sensor_id=int(sensor_id),
        final_state=final_state,
        alert_active=alert_active,
        warning_active=warning_active,
        prediction_current=prediction_current,
    )

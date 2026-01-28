"""Queries para estado consolidado del sensor.

Funciones puras de consulta a BD para obtener alertas, warnings y predicciones.
"""

from __future__ import annotations

import json
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from ..schemas import (
    ActiveAlert,
    ActiveWarning,
    CurrentPrediction,
    SensorFinalState,
)


def get_active_alert(db: Session, sensor_id: int) -> Optional[ActiveAlert]:
    """Obtiene la alerta activa más reciente del sensor."""
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, device_id, threshold_id, severity, status, triggered_value, triggered_at
            FROM dbo.alerts
            WHERE sensor_id = :sensor_id
              AND status = 'active'
            ORDER BY triggered_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return ActiveAlert(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        device_id=int(row.device_id),
        threshold_id=int(row.threshold_id),
        severity=str(row.severity),
        status=str(row.status),
        triggered_value=float(row.triggered_value),
        triggered_at=row.triggered_at,
    )


def get_active_warning(db: Session, sensor_id: int) -> Optional[ActiveWarning]:
    """Obtiene el warning activo más reciente del sensor (delta spike)."""
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, device_id, event_type, event_code, status, created_at, title, message, payload
            FROM dbo.ml_events
            WHERE sensor_id = :sensor_id
              AND event_code = 'DELTA_SPIKE'
              AND status = 'active'
            ORDER BY created_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    payload = None
    try:
        if row.payload is not None:
            payload = json.loads(row.payload)
    except Exception:
        payload = None

    return ActiveWarning(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        device_id=int(row.device_id),
        event_type=str(row.event_type),
        event_code=str(row.event_code),
        status=str(row.status),
        created_at=row.created_at,
        title=str(row.title) if row.title is not None else None,
        message=str(row.message) if row.message is not None else None,
        payload=payload,
    )


def get_current_prediction(db: Session, sensor_id: int) -> Optional[CurrentPrediction]:
    """Obtiene la predicción más reciente del sensor."""
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, model_id, predicted_value, confidence, predicted_at, target_timestamp
            FROM dbo.predictions
            WHERE sensor_id = :sensor_id
            ORDER BY predicted_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return CurrentPrediction(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        model_id=int(row.model_id),
        predicted_value=float(row.predicted_value),
        confidence=float(row.confidence),
        predicted_at=row.predicted_at,
        target_timestamp=row.target_timestamp,
    )


def compute_final_state(
    *,
    alert_active: Optional[ActiveAlert],
    warning_active: Optional[ActiveWarning],
    prediction_current: Optional[CurrentPrediction],
) -> SensorFinalState:
    """Calcula el estado final del sensor basado en alertas/warnings/predicciones.
    
    Prioridad: ALERT > WARNING > PREDICTION > UNKNOWN
    """
    if alert_active is not None:
        return SensorFinalState.ALERT
    if warning_active is not None:
        return SensorFinalState.WARNING
    if prediction_current is not None:
        return SensorFinalState.PREDICTION
    return SensorFinalState.UNKNOWN

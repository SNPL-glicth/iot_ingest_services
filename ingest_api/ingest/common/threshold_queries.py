"""Consultas de umbrales para validación.

Módulo separado de validation.py para mantener archivos <180 líneas.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


# Constantes para validación de warm-up
MIN_READINGS_FOR_DELTA = 3
WARMUP_WINDOW_HOURS = 2


def get_recent_reading_count(db: Session, sensor_id: int, hours: int = WARMUP_WINDOW_HOURS) -> int:
    """Cuenta lecturas recientes del sensor para validación de warm-up."""
    try:
        row = db.execute(
            text(
                """
                SELECT COUNT(*) as cnt
                FROM dbo.sensor_readings
                WHERE sensor_id = :sensor_id
                  AND timestamp >= DATEADD(hour, -:hours, GETDATE())
                """
            ),
            {"sensor_id": sensor_id, "hours": hours},
        ).fetchone()
        return int(row[0]) if row and row[0] else 0
    except Exception:
        return 0


def get_warning_thresholds(db: Session, sensor_id: int) -> tuple[Optional[float], Optional[float]]:
    """Obtiene los umbrales WARNING del usuario para el sensor.
    
    Returns:
        (warning_min, warning_max) o (None, None) si no hay umbrales configurados
    """
    try:
        row = db.execute(
            text(
                """
                SELECT threshold_value_min, threshold_value_max
                FROM dbo.alert_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                  AND severity = 'warning'
                  AND condition_type = 'out_of_range'
                ORDER BY id ASC
                """
            ),
            {"sensor_id": sensor_id},
        ).fetchone()
        
        if not row:
            return None, None
        
        warning_min = float(row[0]) if row[0] is not None else None
        warning_max = float(row[1]) if row[1] is not None else None
        return warning_min, warning_max
    except Exception:
        return None, None


def is_value_within_warning_range(
    value: float,
    warning_min: Optional[float],
    warning_max: Optional[float],
) -> bool:
    """Verifica si el valor está dentro del rango WARNING del usuario."""
    if warning_min is None and warning_max is None:
        return False
    if warning_min is not None and value < warning_min:
        return False
    if warning_max is not None and value > warning_max:
        return False
    return True

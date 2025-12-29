"""Utilidades para manejo de rangos físicos de sensores."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class PhysicalRange:
    """Rango físico del sensor (hard limits)."""

    min_value: Optional[float]
    max_value: Optional[float]
    threshold_id: Optional[int] = None

    def violates(self, value: float) -> bool:
        """Verifica si un valor viola el rango físico."""
        if self.min_value is not None and value < self.min_value:
            return True
        if self.max_value is not None and value > self.max_value:
            return True
        return False


def get_physical_range(db: Session, sensor_id: int) -> Optional[PhysicalRange]:
    """Obtiene el rango físico del sensor desde alert_thresholds.

    Busca un threshold activo con condition_type='out_of_range' que define
    los límites físicos del sensor.
    """
    row = db.execute(
        text(
            """
            SELECT TOP 1
                id,
                threshold_value_min,
                threshold_value_max
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id
              AND is_active = 1
              AND condition_type = 'out_of_range'
            ORDER BY id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    min_val = float(row.threshold_value_min) if row.threshold_value_min is not None else None
    max_val = float(row.threshold_value_max) if row.threshold_value_max is not None else None

    if min_val is None and max_val is None:
        return None

    return PhysicalRange(
        min_value=min_val,
        max_value=max_val,
        threshold_id=int(row.id),
    )


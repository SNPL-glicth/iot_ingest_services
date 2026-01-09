"""Utilidades para manejo de rangos físicos y umbrales de sensores.

FIX AUDITORIA PROBLEMA 5: Soportar TODOS los tipos de condición:
- greater_than: valor > threshold_value_min → ALERTA
- less_than: valor < threshold_value_max → ALERTA  
- out_of_range: valor < min OR valor > max → ALERTA
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, List

from sqlalchemy import text
from sqlalchemy.orm import Session

_logger = logging.getLogger(__name__)


@dataclass
class ThresholdRule:
    """Regla de umbral configurada por el usuario."""

    threshold_id: int
    condition_type: str  # 'greater_than', 'less_than', 'out_of_range'
    min_value: Optional[float]
    max_value: Optional[float]
    severity: str  # 'warning', 'critical'

    def violates(self, value: float) -> bool:
        """Verifica si un valor viola esta regla de umbral."""
        if self.condition_type == 'greater_than':
            # Dispara si valor > threshold_value_min
            if self.min_value is not None and value > self.min_value:
                return True
        elif self.condition_type == 'less_than':
            # Dispara si valor < threshold_value_max
            if self.max_value is not None and value < self.max_value:
                return True
        elif self.condition_type == 'out_of_range':
            # Dispara si valor fuera de [min, max]
            if self.min_value is not None and value < self.min_value:
                return True
            if self.max_value is not None and value > self.max_value:
                return True
        return False


@dataclass
class PhysicalRange:
    """Rango físico del sensor (hard limits) - compatibilidad hacia atrás."""

    min_value: Optional[float]
    max_value: Optional[float]
    threshold_id: Optional[int] = None
    condition_type: str = 'out_of_range'
    severity: str = 'critical'

    def violates(self, value: float) -> bool:
        """Verifica si un valor viola el rango físico."""
        if self.condition_type == 'greater_than':
            if self.min_value is not None and value > self.min_value:
                return True
        elif self.condition_type == 'less_than':
            if self.max_value is not None and value < self.max_value:
                return True
        else:  # out_of_range (default)
            if self.min_value is not None and value < self.min_value:
                return True
            if self.max_value is not None and value > self.max_value:
                return True
        return False


def get_all_thresholds(db: Session, sensor_id: int) -> List[ThresholdRule]:
    """Obtiene TODOS los umbrales activos del sensor.
    
    FIX AUDITORIA: Evalúa greater_than, less_than y out_of_range.
    """
    rows = db.execute(
        text(
            """
            SELECT
                id,
                condition_type,
                threshold_value_min,
                threshold_value_max,
                severity
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id
              AND is_active = 1
            ORDER BY 
                CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchall()

    thresholds = []
    for row in rows:
        min_val = float(row.threshold_value_min) if row.threshold_value_min is not None else None
        max_val = float(row.threshold_value_max) if row.threshold_value_max is not None else None
        
        thresholds.append(ThresholdRule(
            threshold_id=int(row.id),
            condition_type=str(row.condition_type or 'out_of_range'),
            min_value=min_val,
            max_value=max_val,
            severity=str(row.severity or 'warning'),
        ))
    
    return thresholds


def check_threshold_violation(db: Session, sensor_id: int, value: float) -> Optional[ThresholdRule]:
    """Verifica si un valor viola algún umbral configurado.
    
    Retorna el umbral violado de mayor severidad (critical primero).
    """
    thresholds = get_all_thresholds(db, sensor_id)
    
    for threshold in thresholds:
        if threshold.violates(value):
            _logger.info(
                "THRESHOLD VIOLATION sensor_id=%s value=%s threshold_id=%s "
                "condition=%s severity=%s min=%s max=%s",
                sensor_id, value, threshold.threshold_id,
                threshold.condition_type, threshold.severity,
                threshold.min_value, threshold.max_value,
            )
            return threshold
    
    return None


def get_physical_range(db: Session, sensor_id: int) -> Optional[PhysicalRange]:
    """Obtiene el umbral de mayor severidad del sensor.

    FIX AUDITORIA: Ahora evalúa TODOS los tipos de condición, no solo out_of_range.
    Prioriza umbrales 'critical' sobre 'warning'.
    """
    row = db.execute(
        text(
            """
            SELECT TOP 1
                id,
                condition_type,
                threshold_value_min,
                threshold_value_max,
                severity
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id
              AND is_active = 1
            ORDER BY 
                CASE severity WHEN 'critical' THEN 0 ELSE 1 END,
                id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    min_val = float(row.threshold_value_min) if row.threshold_value_min is not None else None
    max_val = float(row.threshold_value_max) if row.threshold_value_max is not None else None
    condition = str(row.condition_type or 'out_of_range')
    severity = str(row.severity or 'critical')

    if min_val is None and max_val is None:
        return None

    return PhysicalRange(
        min_value=min_val,
        max_value=max_val,
        threshold_id=int(row.id),
        condition_type=condition,
        severity=severity,
    )


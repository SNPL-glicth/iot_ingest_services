"""Utilidades para manejo de rangos físicos y umbrales de sensores.

FIX AUDITORIA PROBLEMA 5: Soportar TODOS los tipos de condición:
- greater_than: valor > threshold_value_min → ALERTA
- less_than: valor < threshold_value_max → ALERTA  
- out_of_range: valor < min OR valor > max → ALERTA

FIX 2026-02-02: Agregado caché TTL para evitar queries repetidas (P-2).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple
from collections import OrderedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

_logger = logging.getLogger(__name__)

# Caché de umbrales con TTL
_THRESHOLD_CACHE: OrderedDict[int, Tuple[List["ThresholdRule"], float]] = OrderedDict()
_PHYSICAL_RANGE_CACHE: OrderedDict[int, Tuple[Optional["PhysicalRange"], float]] = OrderedDict()
MAX_CACHE_SIZE = 5000
CACHE_TTL_SECONDS = 60  # 1 minuto


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
    FIX 2026-02-02: Usa caché TTL para evitar queries repetidas (P-2).
    """
    now = time.time()
    
    # Buscar en caché
    cached = _THRESHOLD_CACHE.get(sensor_id)
    if cached is not None:
        thresholds, expires_at = cached
        if expires_at > now:
            _THRESHOLD_CACHE.move_to_end(sensor_id)
            return thresholds
        _THRESHOLD_CACHE.pop(sensor_id, None)
    
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
    
    # Guardar en caché
    while len(_THRESHOLD_CACHE) >= MAX_CACHE_SIZE:
        _THRESHOLD_CACHE.popitem(last=False)
    _THRESHOLD_CACHE[sensor_id] = (thresholds, now + CACHE_TTL_SECONDS)
    
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
    FIX 2026-02-02: Usa caché TTL para evitar queries repetidas (P-2).
    """
    now = time.time()
    
    # Buscar en caché
    cached = _PHYSICAL_RANGE_CACHE.get(sensor_id)
    if cached is not None:
        physical_range, expires_at = cached
        if expires_at > now:
            _PHYSICAL_RANGE_CACHE.move_to_end(sensor_id)
            return physical_range
        _PHYSICAL_RANGE_CACHE.pop(sensor_id, None)
    
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
        # Guardar None en caché para evitar queries repetidas
        while len(_PHYSICAL_RANGE_CACHE) >= MAX_CACHE_SIZE:
            _PHYSICAL_RANGE_CACHE.popitem(last=False)
        _PHYSICAL_RANGE_CACHE[sensor_id] = (None, now + CACHE_TTL_SECONDS)
        return None

    physical_range = PhysicalRange(
        min_value=min_val,
        max_value=max_val,
        threshold_id=int(row.id),
        condition_type=condition,
        severity=severity,
    )
    
    # Guardar en caché
    while len(_PHYSICAL_RANGE_CACHE) >= MAX_CACHE_SIZE:
        _PHYSICAL_RANGE_CACHE.popitem(last=False)
    _PHYSICAL_RANGE_CACHE[sensor_id] = (physical_range, now + CACHE_TTL_SECONDS)
    
    return physical_range


def clear_threshold_cache() -> None:
    """Limpia el caché de umbrales (útil para testing)."""
    _THRESHOLD_CACHE.clear()
    _PHYSICAL_RANGE_CACHE.clear()


def get_threshold_cache_stats() -> dict:
    """Estadísticas del caché de umbrales."""
    return {
        "threshold_cache_size": len(_THRESHOLD_CACHE),
        "physical_range_cache_size": len(_PHYSICAL_RANGE_CACHE),
        "max_size": MAX_CACHE_SIZE,
        "ttl_seconds": CACHE_TTL_SECONDS,
    }


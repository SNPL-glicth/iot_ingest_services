"""Validaciones comunes para los pipelines.

Refactorizado 2026-02-02: Separado en módulos <180 líneas.
- threshold_queries.py: Consultas de umbrales
- suspicious_readings.py: Detección de lecturas sospechosas

MODELO DE ESTADOS:
Todas las validaciones usan SensorStateManager como único punto de decisión
para determinar si un sensor puede generar WARNING/ALERT.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from .physical_ranges import PhysicalRange, get_physical_range
from .delta_utils import DeltaThreshold, LastReading, get_delta_threshold, get_last_reading, check_delta_spike
from .threshold_queries import get_warning_thresholds, is_value_within_warning_range
from .suspicious_readings import is_suspicious_zero_reading, log_suspicious_reading

from ...classification import SensorStateManager

logger = logging.getLogger(__name__)


def validate_alert_data(
    db: Session,
    sensor_id: int,
    value: float,
) -> tuple[bool, Optional[PhysicalRange], str]:
    """Valida que los datos pertenezcan al pipeline de ALERTAS.

    Returns:
        (is_valid, physical_range, reason)
    """
    physical_range = get_physical_range(db, sensor_id)
    if not physical_range:
        return False, None, "No hay rango físico configurado para este sensor"

    if not physical_range.violates(value):
        return False, None, f"Valor {value} no viola el rango físico [{physical_range.min_value}, {physical_range.max_value}]"

    return True, physical_range, f"Valor {value} fuera de rango físico [{physical_range.min_value}, {physical_range.max_value}]"


def validate_warning_data(
    db: Session,
    sensor_id: int,
    value: float,
    current_ts: Optional[datetime] = None,
) -> tuple[bool, Optional[dict], str]:
    """Valida que los datos pertenezcan al pipeline de WARNINGS.
    
    MODELO DE ESTADOS:
    Usa SensorStateManager como único punto de decisión.
    
    REGLA DE DOMINIO CRÍTICA:
    Si el valor está dentro del rango WARNING del usuario, NO puede haber
    delta spike. El usuario definió ese rango como "normal".
    
    Requiere:
    0. Sensor en estado que permite eventos (NORMAL, WARNING, ALERT)
    1. Valor FUERA del rango WARNING del usuario
    2. Lectura previa reciente (máx 10 minutos)
    3. Umbrales de delta configurados
    4. Delta spike detectado

    Returns:
        (is_valid, delta_info, reason)
    """
    from datetime import datetime, timezone

    if current_ts is None:
        current_ts = datetime.now(timezone.utc)

    # =========================================================================
    # PASO 0: MODELO DE ESTADOS - Verificar si sensor puede generar eventos
    # =========================================================================
    state_manager = SensorStateManager(db)
    can_generate, state_reason = state_manager.can_generate_events(sensor_id)
    
    if not can_generate:
        return False, None, f"Sensor bloqueado para eventos: {state_reason}"

    # =========================================================================
    # PASO 1: REGLA DE DOMINIO - Verificar umbrales del usuario
    # Si el valor está dentro del rango WARNING, NO puede haber delta spike.
    # =========================================================================
    warning_min, warning_max = get_warning_thresholds(db, sensor_id)
    if is_value_within_warning_range(value, warning_min, warning_max):
        return False, None, "Valor dentro del rango WARNING del usuario; delta spike no aplica"

    # PASO 2: Verificar que existe lectura previa RECIENTE
    last_reading = get_last_reading(db, sensor_id)
    if not last_reading:
        return False, None, "Sin historial reciente para evaluar delta spike"

    # PASO 3: Verificar umbrales de delta configurados
    delta_threshold = get_delta_threshold(db, sensor_id)
    if not delta_threshold:
        return False, None, "No hay umbrales de delta configurados para este sensor"

    # PASO 4: Evaluar delta spike
    delta_info = check_delta_spike(
        current_value=value,
        current_ts=current_ts,
        last_reading=last_reading,
        delta_threshold=delta_threshold,
    )

    if not delta_info or not delta_info.get("is_spike", False):
        return False, None, "No se detectó delta spike significativo"

    return True, delta_info, f"Delta spike detectado: {delta_info.get('reason', '')}"


def validate_prediction_data(
    db: Session,
    sensor_id: int,
    value: float,
    current_ts: Optional[datetime] = None,
) -> tuple[bool, str]:
    """Valida que los datos pertenezcan al pipeline de PREDICCIONES.

    MODELO DE ESTADOS:
    Si el sensor está en INITIALIZING o STALE, el dato es LIMPIO (no puede
    haber delta spike sin historial válido).

    Los datos de predicción deben estar limpios:
    - Sin violación física
    - Sin delta spike reciente (SOLO si sensor puede generar eventos)

    Returns:
        (is_valid, reason)
    """
    from datetime import datetime, timezone

    # Verificar que NO haya violación física
    physical_range = get_physical_range(db, sensor_id)
    if physical_range and physical_range.violates(value):
        return False, f"Valor {value} viola rango físico - debe ir a ALERT pipeline"

    # Verificar delta spike SOLO si sensor puede generar eventos
    if current_ts is None:
        current_ts = datetime.now(timezone.utc)

    # MODELO DE ESTADOS: Si sensor no puede generar eventos, dato es limpio
    state_manager = SensorStateManager(db)
    can_generate, state_reason = state_manager.can_generate_events(sensor_id)
    
    if not can_generate:
        # Sensor en INITIALIZING/STALE → no puede haber delta spike → dato limpio
        return True, f"Dato limpio ({state_reason}), apto para ML"

    # Sensor puede generar eventos, verificar delta spike
    last_reading = get_last_reading(db, sensor_id)
    if not last_reading:
        return True, "Dato limpio (sin historial reciente), apto para ML"

    delta_threshold = get_delta_threshold(db, sensor_id)
    if delta_threshold:
        delta_info = check_delta_spike(
            current_value=value,
            current_ts=current_ts,
            last_reading=last_reading,
            delta_threshold=delta_threshold,
        )
        if delta_info and delta_info.get("is_spike", False):
            return False, "Delta spike detectado - debe ir a WARNING pipeline"

    return True, "Dato limpio, apto para ML"


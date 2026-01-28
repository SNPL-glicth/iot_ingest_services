"""Validaciones comunes para los pipelines.

MODELO DE ESTADOS:
Todas las validaciones usan SensorStateManager como único punto de decisión
para determinar si un sensor puede generar WARNING/ALERT.

Regla de dominio:
- Sensor en INITIALIZING → NUNCA genera eventos
- Sensor en STALE → NUNCA genera eventos  
- Sensor en NORMAL → puede generar WARNING/ALERT
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from .physical_ranges import PhysicalRange, get_physical_range
from .delta_utils import DeltaThreshold, LastReading, get_delta_threshold, get_last_reading, check_delta_spike

# FIX MODELO DE ESTADOS: Importar gestor de estado operacional
from iot_ingest_services.ingest_api.classification.sensor_state import SensorStateManager

logger = logging.getLogger(__name__)

# FIX 1: Tipos de sensor donde 0.00000 exacto es sospechoso
_SUSPICIOUS_ZERO_SENSOR_TYPES = frozenset({
    "temperature",
    "humidity", 
    "pressure",
    "ph",
})


def is_suspicious_zero_reading(
    value: float,
    sensor_type: Optional[str] = None,
    tolerance: float = 1e-6,
) -> bool:
    """Detecta si un valor 0.00000 es sospechoso según el tipo de sensor.
    
    Para sensores de temperatura, humedad, presión, pH, un valor exactamente 0
    es muy improbable en condiciones reales y puede indicar:
    - Sensor desconectado
    - Error de lectura
    - Placeholder del simulador
    
    Args:
        value: Valor de la lectura
        sensor_type: Tipo de sensor (opcional)
        tolerance: Tolerancia para considerar "cero exacto"
    
    Returns:
        True si el valor es sospechoso, False si es válido
    """
    if abs(value) > tolerance:
        return False
    
    # Si no tenemos tipo de sensor, asumimos que podría ser sospechoso
    if sensor_type is None:
        return True
    
    # Solo marcar como sospechoso para tipos específicos
    normalized_type = sensor_type.lower().strip()
    return normalized_type in _SUSPICIOUS_ZERO_SENSOR_TYPES


def log_suspicious_reading(
    sensor_id: int,
    value: float,
    sensor_type: Optional[str] = None,
    reason: str = "zero_value",
) -> None:
    """Registra una lectura sospechosa para análisis posterior."""
    logger.warning(
        f"SUSPICIOUS_READING sensor_id={sensor_id} value={value} "
        f"sensor_type={sensor_type} reason={reason}"
    )


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


# FIX RAÍZ: Constantes para validación de warm-up
MIN_READINGS_FOR_DELTA = 3  # Mínimo de lecturas recientes para evaluar delta spike
WARMUP_WINDOW_HOURS = 2  # Ventana de tiempo para contar lecturas de warm-up


def _get_recent_reading_count(db: Session, sensor_id: int, hours: int = WARMUP_WINDOW_HOURS) -> int:
    """Cuenta lecturas recientes del sensor para validación de warm-up."""
    from sqlalchemy import text
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


def _get_warning_thresholds(db: Session, sensor_id: int) -> tuple[Optional[float], Optional[float]]:
    """Obtiene los umbrales WARNING del usuario para el sensor.
    
    Returns:
        (warning_min, warning_max) o (None, None) si no hay umbrales configurados
    """
    from sqlalchemy import text
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


def _is_value_within_warning_range(value: float, warning_min: Optional[float], warning_max: Optional[float]) -> bool:
    """Verifica si el valor está dentro del rango WARNING del usuario."""
    if warning_min is None and warning_max is None:
        return False
    if warning_min is not None and value < warning_min:
        return False
    if warning_max is not None and value > warning_max:
        return False
    return True


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
    warning_min, warning_max = _get_warning_thresholds(db, sensor_id)
    if _is_value_within_warning_range(value, warning_min, warning_max):
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


"""Guard rails para rechazo temprano de datos inválidos.

Este módulo implementa validaciones estrictas que rechazan datos
antes de que entren al pipeline de ingesta.

Principios:
- Fail fast: Rechazar lo antes posible
- Explicitar razón del rechazo
- Loggear para auditoría
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Resultado de una validación."""
    is_valid: bool
    reason: str
    details: Optional[dict] = None


# Rangos físicos absolutos por tipo de sensor (hard limits)
PHYSICAL_LIMITS = {
    "temperature": {"min": -100, "max": 500, "unit": "°C"},
    "humidity": {"min": 0, "max": 100, "unit": "%"},
    "pressure": {"min": 0, "max": 2000, "unit": "hPa"},
    "air_quality": {"min": 0, "max": 10000, "unit": "ppm"},
    "voltage": {"min": 0, "max": 1000, "unit": "V"},
    "power": {"min": 0, "max": 1000000, "unit": "W"},
    "ph": {"min": 0, "max": 14, "unit": "pH"},
}

# Timestamps: máximo desfase aceptable
MAX_FUTURE_SECONDS = 300  # 5 minutos en el futuro
MAX_PAST_DAYS = 30  # 30 días en el pasado


def validate_reading_value(
    value: float,
    sensor_type: Optional[str] = None,
) -> ValidationResult:
    """Valida que un valor de lectura sea físicamente posible.
    
    Args:
        value: Valor de la lectura
        sensor_type: Tipo de sensor (opcional, para validación específica)
    
    Returns:
        ValidationResult con is_valid=True si el valor es aceptable
    """
    # Guard 1: NaN o infinito
    if value != value:  # NaN check
        return ValidationResult(
            is_valid=False,
            reason="VALUE_IS_NAN",
            details={"value": str(value)},
        )
    
    if abs(value) == float('inf'):
        return ValidationResult(
            is_valid=False,
            reason="VALUE_IS_INFINITE",
            details={"value": str(value)},
        )
    
    # Guard 2: Valor fuera de rangos físicos absolutos
    if sensor_type:
        normalized_type = sensor_type.lower().strip()
        limits = PHYSICAL_LIMITS.get(normalized_type)
        if limits:
            if value < limits["min"] or value > limits["max"]:
                return ValidationResult(
                    is_valid=False,
                    reason="VALUE_OUTSIDE_PHYSICAL_LIMITS",
                    details={
                        "value": value,
                        "sensor_type": normalized_type,
                        "min": limits["min"],
                        "max": limits["max"],
                        "unit": limits["unit"],
                    },
                )
    
    return ValidationResult(is_valid=True, reason="OK")


def validate_timestamp(
    ts: Optional[datetime],
    reference_time: Optional[datetime] = None,
) -> ValidationResult:
    """Valida que un timestamp sea razonable.
    
    Args:
        ts: Timestamp a validar
        reference_time: Tiempo de referencia (default: ahora UTC)
    
    Returns:
        ValidationResult con is_valid=True si el timestamp es aceptable
    """
    if ts is None:
        return ValidationResult(is_valid=True, reason="OK_NULL_TIMESTAMP")
    
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)
    
    # Asegurar que ambos tengan timezone para comparación
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    
    # Guard 1: Timestamp muy en el futuro
    max_future = reference_time + timedelta(seconds=MAX_FUTURE_SECONDS)
    if ts > max_future:
        return ValidationResult(
            is_valid=False,
            reason="TIMESTAMP_IN_FUTURE",
            details={
                "timestamp": ts.isoformat(),
                "reference": reference_time.isoformat(),
                "max_allowed": max_future.isoformat(),
            },
        )
    
    # Guard 2: Timestamp muy en el pasado
    max_past = reference_time - timedelta(days=MAX_PAST_DAYS)
    if ts < max_past:
        return ValidationResult(
            is_valid=False,
            reason="TIMESTAMP_TOO_OLD",
            details={
                "timestamp": ts.isoformat(),
                "reference": reference_time.isoformat(),
                "min_allowed": max_past.isoformat(),
            },
        )
    
    return ValidationResult(is_valid=True, reason="OK")


def validate_sensor_id(sensor_id: int) -> ValidationResult:
    """Valida que un sensor_id sea válido.
    
    Args:
        sensor_id: ID del sensor
    
    Returns:
        ValidationResult con is_valid=True si el ID es válido
    """
    if sensor_id is None:
        return ValidationResult(
            is_valid=False,
            reason="SENSOR_ID_NULL",
        )
    
    if not isinstance(sensor_id, int) or sensor_id <= 0:
        return ValidationResult(
            is_valid=False,
            reason="SENSOR_ID_INVALID",
            details={"sensor_id": sensor_id},
        )
    
    return ValidationResult(is_valid=True, reason="OK")


def guard_reading(
    sensor_id: int,
    value: float,
    device_timestamp: Optional[datetime] = None,
    sensor_type: Optional[str] = None,
) -> ValidationResult:
    """Guard rail principal para lecturas.
    
    Ejecuta todas las validaciones y retorna el primer error encontrado.
    
    Args:
        sensor_id: ID del sensor
        value: Valor de la lectura
        device_timestamp: Timestamp del dispositivo (opcional)
        sensor_type: Tipo de sensor (opcional)
    
    Returns:
        ValidationResult con is_valid=True si pasa todas las validaciones
    """
    # 1. Validar sensor_id
    result = validate_sensor_id(sensor_id)
    if not result.is_valid:
        logger.warning(
            "GUARD_REJECT sensor_id=%s reason=%s details=%s",
            sensor_id, result.reason, result.details,
        )
        return result
    
    # 2. Validar valor
    result = validate_reading_value(value, sensor_type)
    if not result.is_valid:
        logger.warning(
            "GUARD_REJECT sensor_id=%s value=%s reason=%s details=%s",
            sensor_id, value, result.reason, result.details,
        )
        return result
    
    # 3. Validar timestamp
    result = validate_timestamp(device_timestamp)
    if not result.is_valid:
        logger.warning(
            "GUARD_REJECT sensor_id=%s timestamp=%s reason=%s details=%s",
            sensor_id, device_timestamp, result.reason, result.details,
        )
        return result
    
    return ValidationResult(is_valid=True, reason="OK")

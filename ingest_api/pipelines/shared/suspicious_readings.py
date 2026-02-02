"""Detección de lecturas sospechosas.

Módulo separado de validation.py para mantener archivos <180 líneas.
"""

from __future__ import annotations

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Tipos de sensor donde 0.00000 exacto es sospechoso
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

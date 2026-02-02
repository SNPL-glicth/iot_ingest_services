"""Validador de lecturas de dominio."""

from __future__ import annotations

import logging
from typing import Optional, Tuple

from ..domain.reading import Reading

logger = logging.getLogger(__name__)

# Rangos físicos por tipo de sensor
PHYSICAL_RANGES = {
    "temperature": (-50.0, 100.0),
    "humidity": (0.0, 100.0),
    "pressure": (800.0, 1200.0),
    "co2": (0.0, 10000.0),
    "unknown": (-1e9, 1e9),
}


class ReadingValidator:
    """Valida lecturas de dominio antes de procesarlas.
    
    Responsabilidades:
    - Validar rangos físicos
    - Detectar valores sospechosos
    - Validar consistencia temporal
    """
    
    def validate(self, reading: Reading) -> Tuple[bool, Optional[str]]:
        """Valida una lectura de dominio.
        
        Args:
            reading: Lectura a validar
            
        Returns:
            (is_valid, error_message)
        """
        # Validar sensor_id
        if reading.sensor_id <= 0:
            return False, "sensor_id must be positive"
        
        # Validar rango físico
        is_in_range, range_error = self._validate_physical_range(reading)
        if not is_in_range:
            return False, range_error
        
        # Detectar valores sospechosos
        if self._is_suspicious_value(reading.value):
            logger.warning(
                "[VALIDATOR] Suspicious value: sensor_id=%d value=%f",
                reading.sensor_id,
                reading.value,
            )
        
        return True, None
    
    def _validate_physical_range(self, reading: Reading) -> Tuple[bool, Optional[str]]:
        """Valida que el valor esté en rango físico."""
        sensor_type = reading.sensor_type.lower()
        min_val, max_val = PHYSICAL_RANGES.get(sensor_type, PHYSICAL_RANGES["unknown"])
        
        if reading.value < min_val or reading.value > max_val:
            return False, f"Value {reading.value} out of range [{min_val}, {max_val}] for {sensor_type}"
        
        return True, None
    
    def _is_suspicious_value(self, value: float) -> bool:
        """Detecta valores sospechosos (ej: exactamente 0.00000)."""
        return abs(value) < 1e-10

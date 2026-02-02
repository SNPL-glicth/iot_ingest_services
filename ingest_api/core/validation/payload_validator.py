"""Validador de payloads MQTT."""

from __future__ import annotations

import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Campos requeridos en payload MQTT
REQUIRED_FIELDS = ["sensorId", "value", "timestamp"]

# Versión de contrato soportada
SUPPORTED_VERSIONS = [1]


class PayloadValidator:
    """Valida payloads MQTT entrantes.
    
    Responsabilidades:
    - Validar campos requeridos
    - Validar tipos de datos
    - Validar versión de contrato
    """
    
    def validate(self, data: dict) -> Tuple[bool, Optional[str]]:
        """Valida un payload MQTT.
        
        Args:
            data: Payload parseado
            
        Returns:
            (is_valid, error_message)
        """
        if not isinstance(data, dict):
            return False, "Payload must be a dictionary"
        
        # Validar versión
        version = data.get("v", 1)
        if version not in SUPPORTED_VERSIONS:
            return False, f"Unsupported version: {version}"
        
        # Validar campos requeridos
        for field in REQUIRED_FIELDS:
            if field not in data:
                return False, f"Missing required field: {field}"
            if data[field] is None:
                return False, f"Field cannot be null: {field}"
        
        # Validar tipos
        if not self._validate_sensor_id(data.get("sensorId")):
            return False, "sensorId must be a valid integer or numeric string"
        
        if not self._validate_value(data.get("value")):
            return False, "value must be a number"
        
        if not self._validate_timestamp(data.get("timestamp")):
            return False, "timestamp must be a non-empty string"
        
        return True, None
    
    def _validate_sensor_id(self, sensor_id) -> bool:
        """Valida sensor_id."""
        if sensor_id is None:
            return False
        try:
            int(sensor_id)
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_value(self, value) -> bool:
        """Valida value."""
        if value is None:
            return False
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _validate_timestamp(self, timestamp) -> bool:
        """Valida timestamp."""
        return isinstance(timestamp, str) and len(timestamp) > 0

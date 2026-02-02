"""Contratos de dominio para validación de datos."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, List


@dataclass
class MQTTPayload:
    """Contrato del payload MQTT entrante."""
    v: int  # Versión del contrato
    sensorId: str
    value: float
    timestamp: str
    type: str = "reading"
    metadata: Optional[dict] = None
    
    @property
    def sensor_type(self) -> str:
        """Extrae sensor_type de metadata."""
        if self.metadata and "sensorType" in self.metadata:
            return self.metadata["sensorType"]
        return "unknown"
    
    @property
    def device_uuid(self) -> Optional[str]:
        """Extrae device_uuid de metadata."""
        if self.metadata:
            return self.metadata.get("deviceUuid")
        return None
    
    @property
    def sensor_uuid(self) -> Optional[str]:
        """Extrae sensor_uuid de metadata."""
        if self.metadata:
            return self.metadata.get("sensorUuid")
        return None


@dataclass
class DomainContract:
    """Contrato canónico de dominio.
    
    Define los campos obligatorios y opcionales que debe tener
    una lectura para ser procesada por el pipeline.
    """
    # Campos obligatorios
    REQUIRED_FIELDS: List[str] = None
    
    # Campos opcionales con defaults
    OPTIONAL_FIELDS: dict = None
    
    def __post_init__(self):
        self.REQUIRED_FIELDS = [
            "sensor_id",
            "value", 
            "timestamp",
        ]
        self.OPTIONAL_FIELDS = {
            "device_timestamp": None,
            "sensor_type": "unknown",
            "device_uuid": None,
            "sensor_uuid": None,
        }
    
    def validate(self, data: dict) -> tuple[bool, Optional[str]]:
        """Valida que los datos cumplan el contrato.
        
        Returns:
            (is_valid, error_message)
        """
        for field in self.REQUIRED_FIELDS:
            if field not in data or data[field] is None:
                return False, f"Missing required field: {field}"
        
        # Validar tipos
        if not isinstance(data.get("sensor_id"), int):
            try:
                int(data["sensor_id"])
            except (ValueError, TypeError):
                return False, "sensor_id must be an integer"
        
        if not isinstance(data.get("value"), (int, float)):
            try:
                float(data["value"])
            except (ValueError, TypeError):
                return False, "value must be a number"
        
        return True, None

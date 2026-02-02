"""Adaptador MQTT → Modelo de Dominio."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from ..domain.reading import Reading, ReadingStatus
from ..validation.payload_validator import PayloadValidator

logger = logging.getLogger(__name__)


class MQTTAdapter:
    """Adapta payloads MQTT al modelo de dominio.
    
    Responsabilidades:
    - Validación de payload MQTT
    - Conversión a modelo Reading
    - Extracción de metadata
    """
    
    def __init__(self):
        self._validator = PayloadValidator()
    
    def to_reading(self, data: dict) -> Optional[Reading]:
        """Convierte payload MQTT a Reading de dominio.
        
        Args:
            data: Payload MQTT parseado
            
        Returns:
            Reading si es válido, None si no
        """
        # Validar payload
        is_valid, error = self._validator.validate(data)
        if not is_valid:
            logger.warning("[ADAPTER] Validation failed: %s", error)
            return None
        
        try:
            # Extraer sensor_id
            sensor_id = self._extract_sensor_id(data)
            if sensor_id is None:
                logger.warning("[ADAPTER] Invalid sensor_id: %s", data.get("sensorId"))
                return None
            
            # Extraer timestamp
            timestamp = self._parse_timestamp(data.get("timestamp", ""))
            device_timestamp = timestamp
            
            # Extraer metadata
            metadata = data.get("metadata", {}) or {}
            sensor_type = metadata.get("sensorType", "unknown")
            device_uuid = metadata.get("deviceUuid")
            sensor_uuid = metadata.get("sensorUuid")
            
            return Reading(
                sensor_id=sensor_id,
                value=float(data["value"]),
                timestamp=datetime.utcnow(),
                device_timestamp=device_timestamp,
                sensor_type=sensor_type,
                device_uuid=device_uuid,
                sensor_uuid=sensor_uuid,
                status=ReadingStatus.VALIDATED,
            )
            
        except Exception as e:
            logger.exception("[ADAPTER] Conversion error: %s", e)
            return None
    
    def _extract_sensor_id(self, data: dict) -> Optional[int]:
        """Extrae y valida sensor_id."""
        sensor_id = data.get("sensorId")
        if sensor_id is None:
            return None
        
        try:
            return int(sensor_id)
        except (ValueError, TypeError):
            return None
    
    def _parse_timestamp(self, ts_str: str) -> Optional[datetime]:
        """Parsea timestamp ISO8601."""
        if not ts_str:
            return None
        
        try:
            return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except Exception:
            return None

"""Validadores de payloads MQTT para ingesta.

Valida y transforma mensajes MQTT al formato interno.

FIX 2026-02-02: Corregido parsing repetido de timestamp. Ahora se parsea
una sola vez en el validator y se cachea el resultado.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field, validator, root_validator

logger = logging.getLogger(__name__)


class MQTTReadingPayload(BaseModel):
    """Schema de validación para lecturas MQTT.
    
    Formato esperado:
    {
        "v": 1,
        "sensorId": "42",
        "value": 23.456,
        "timestamp": "2026-01-31T08:00:00.123456Z",
        "type": "reading",
        "metadata": {
            "deviceId": "10",
            "deviceUuid": "d1234567-...",
            "sensorUuid": "s7654321-...",
            "sequence": 12345
        }
    }
    """
    
    v: int = Field(default=1, ge=1, le=10)
    sensor_id: str = Field(..., alias="sensorId")
    value: float
    timestamp: str
    type: str = "reading"
    metadata: dict[str, Any] = Field(default_factory=dict)
    msg_id: Optional[str] = Field(default=None, alias="msgId")
    
    @validator("value")
    def validate_value(cls, v):
        if v != v:  # NaN check
            raise ValueError("Value is NaN")
        if v == float("inf") or v == float("-inf"):
            raise ValueError("Value is infinite")
        if not (-1e12 < v < 1e12):
            raise ValueError("Value out of range")
        return v
    
    # Campos cacheados (se calculan una vez en root_validator)
    _parsed_ts: Optional[float] = None
    _parsed_dt: Optional[datetime] = None
    
    class Config:
        populate_by_name = True
        underscore_attrs_are_private = True
    
    @validator("timestamp")
    def validate_timestamp(cls, v):
        try:
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
            ts = dt.timestamp()
            now = time.time()
            
            if ts > now + 300:
                raise ValueError("Timestamp too far in future (>5 min)")
            if ts < now - 86400:
                raise ValueError("Timestamp too old (>24 hours)")
            
            return v
        except Exception as e:
            raise ValueError(f"Invalid timestamp format: {e}")
    
    @root_validator(pre=False)
    def cache_parsed_timestamp(cls, values):
        """Parsea timestamp una sola vez y cachea el resultado."""
        ts_str = values.get("timestamp")
        if ts_str:
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                values["_parsed_dt"] = dt
                values["_parsed_ts"] = dt.timestamp()
            except Exception:
                values["_parsed_ts"] = time.time()
                values["_parsed_dt"] = datetime.now(timezone.utc)
        return values
    
    @validator("sensor_id")
    def validate_sensor_id(cls, v):
        if not v or not v.strip():
            raise ValueError("sensorId is required")
        return v.strip()
    
    @property
    def sensor_id_int(self) -> Optional[int]:
        """Intenta convertir sensor_id a int."""
        try:
            return int(self.sensor_id)
        except ValueError:
            return None
    
    @property
    def device_id(self) -> Optional[str]:
        return self.metadata.get("deviceId")
    
    @property
    def device_uuid(self) -> Optional[str]:
        return self.metadata.get("deviceUuid")
    
    @property
    def sensor_uuid(self) -> Optional[str]:
        return self.metadata.get("sensorUuid")
    
    @property
    def sequence(self) -> Optional[int]:
        seq = self.metadata.get("sequence")
        if seq is not None:
            try:
                return int(seq)
            except (ValueError, TypeError):
                return None
        return None
    
    @property
    def sensor_type(self) -> Optional[str]:
        return self.metadata.get("sensorType")
    
    @property
    def timestamp_float(self) -> float:
        """Retorna timestamp como float Unix epoch (cacheado)."""
        if self._parsed_ts is not None:
            return self._parsed_ts
        # Fallback si no se cacheó
        try:
            dt = datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            return time.time()
    
    @property
    def timestamp_datetime(self) -> datetime:
        """Retorna timestamp como datetime (cacheado)."""
        if self._parsed_dt is not None:
            return self._parsed_dt
        # Fallback si no se cacheó
        try:
            return datetime.fromisoformat(self.timestamp.replace("Z", "+00:00"))
        except Exception:
            return datetime.now(timezone.utc)
    
    def to_ingest_row(self) -> dict[str, Any]:
        """Convierte a formato de fila de ingesta."""
        return {
            "sensor_id": self.sensor_id_int,
            "value": self.value,
            "ingested_ts": time.time(),
            "sequence": self.sequence,
            "sensor_ts": self.timestamp_float,
            "device_timestamp": self.timestamp_datetime,
            "msg_id": self.msg_id,
        }


@dataclass
class ValidationResult:
    """Resultado de validación."""
    
    valid: bool
    payload: Optional[MQTTReadingPayload] = None
    error: Optional[str] = None
    warnings: list[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []


def validate_mqtt_reading(data: dict[str, Any]) -> ValidationResult:
    """Valida payload de lectura MQTT.
    
    Args:
        data: Diccionario con datos del mensaje MQTT
        
    Returns:
        ValidationResult con payload validado o error
    """
    warnings = []
    
    try:
        if "sensorId" not in data and "sensor_id" in data:
            data["sensorId"] = data.pop("sensor_id")
            warnings.append("Used snake_case sensor_id instead of sensorId")
        
        if "msgId" not in data and "msg_id" in data:
            data["msgId"] = data.pop("msg_id")
        
        payload = MQTTReadingPayload(**data)
        
        if payload.sensor_id_int is None:
            return ValidationResult(
                valid=False,
                error=f"sensorId must be numeric, got: {payload.sensor_id}"
            )
        
        return ValidationResult(
            valid=True,
            payload=payload,
            warnings=warnings,
        )
        
    except Exception as e:
        logger.warning("[MQTT_VALIDATOR] Validation failed: %s", e)
        return ValidationResult(
            valid=False,
            error=str(e),
        )

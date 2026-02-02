"""Modelo de dominio para lecturas de sensores."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class ReadingStatus(Enum):
    """Estado de procesamiento de una lectura."""
    PENDING = "pending"
    VALIDATED = "validated"
    PROCESSED = "processed"
    FAILED = "failed"
    REJECTED = "rejected"


@dataclass
class Reading:
    """Lectura de sensor - modelo canónico de dominio.
    
    Este es el contrato único que fluye por todo el pipeline:
    MQTT → Validación → SP → Redis → ML
    """
    sensor_id: int
    value: float
    timestamp: datetime
    device_timestamp: Optional[datetime] = None
    sensor_type: str = "unknown"
    status: ReadingStatus = ReadingStatus.PENDING
    
    # Metadata opcional
    device_uuid: Optional[str] = None
    sensor_uuid: Optional[str] = None
    sequence: Optional[int] = None
    
    # Timestamps de procesamiento
    received_at: datetime = field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    
    def to_sp_params(self) -> dict:
        """Convierte a parámetros para el SP."""
        return {
            "sensor_id": self.sensor_id,
            "value": float(self.value),
            "device_ts": self.device_timestamp,
        }
    
    def to_redis_data(self) -> dict:
        """Convierte a formato Redis Stream."""
        ts = self.device_timestamp or self.timestamp
        return {
            "sensor_id": str(self.sensor_id),
            "sensor_type": self.sensor_type,
            "value": str(self.value),
            "timestamp": str(ts.timestamp() if isinstance(ts, datetime) else ts),
        }
    
    def mark_processed(self):
        """Marca la lectura como procesada."""
        self.status = ReadingStatus.PROCESSED
        self.processed_at = datetime.utcnow()
    
    def mark_failed(self):
        """Marca la lectura como fallida."""
        self.status = ReadingStatus.FAILED
        self.processed_at = datetime.utcnow()

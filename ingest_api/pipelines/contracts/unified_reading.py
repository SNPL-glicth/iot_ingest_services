"""DTO unificado para lecturas de sensores.

Proporciona un formato consistente para todos los canales de entrada:
- HTTP (legacy y packets)
- MQTT
- Redis Streams
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field, validator

if TYPE_CHECKING:
    from ...mqtt.validators import MQTTReadingPayload
    from ...schemas import SensorReadingUuidIn


class ReadingSource(str, Enum):
    """Origen de la lectura."""
    HTTP_LEGACY = "http_legacy"
    HTTP_PACKET = "http_packet"
    MQTT = "mqtt"
    REDIS = "redis"
    UNKNOWN = "unknown"


class UnifiedReading(BaseModel):
    """DTO unificado para lecturas de sensores.
    
    Todos los campos tienen tipos consistentes independientemente del origen.
    
    Attributes:
        sensor_id: ID numérico del sensor (siempre int)
        value: Valor de la lectura (siempre float)
        timestamp: Timestamp Unix de la lectura (siempre float)
        sensor_type: Tipo de sensor (opcional)
        sequence: Número de secuencia para ordenamiento (opcional)
        msg_id: ID único del mensaje para deduplicación (opcional)
        source: Origen de la lectura
        device_timestamp: Timestamp del dispositivo como datetime (opcional)
        ingested_ts: Timestamp de ingesta (cuando llegó al servicio)
    """
    
    sensor_id: int = Field(..., ge=1, description="ID numérico del sensor")
    value: float = Field(..., description="Valor de la lectura")
    timestamp: float = Field(..., description="Timestamp Unix de la lectura")
    sensor_type: Optional[str] = Field(None, description="Tipo de sensor")
    sequence: Optional[int] = Field(None, ge=0, description="Número de secuencia")
    msg_id: Optional[str] = Field(None, description="ID único del mensaje")
    source: ReadingSource = Field(ReadingSource.UNKNOWN, description="Origen")
    device_timestamp: Optional[datetime] = Field(None, description="Timestamp del dispositivo")
    ingested_ts: float = Field(default_factory=time.time, description="Timestamp de ingesta")
    
    class Config:
        use_enum_values = True
    
    @validator("value")
    def validate_value(cls, v):
        """Valida que el valor no sea NaN ni infinito."""
        if v != v:  # NaN check
            raise ValueError("Value is NaN")
        if v == float("inf") or v == float("-inf"):
            raise ValueError("Value is infinite")
        return v
    
    @validator("timestamp")
    def validate_timestamp(cls, v):
        """Valida que el timestamp sea razonable."""
        now = time.time()
        # No más de 5 minutos en el futuro
        if v > now + 300:
            raise ValueError("Timestamp too far in future (>5 min)")
        # No más de 30 días en el pasado
        if v < now - (30 * 24 * 3600):
            raise ValueError("Timestamp too old (>30 days)")
        return v
    
    def get_msg_id(self) -> str:
        """Obtiene o genera un msg_id para deduplicación.
        
        Returns:
            msg_id existente o generado basado en sensor_id + timestamp + value
        """
        if self.msg_id:
            return self.msg_id
        return f"{self.sensor_id}:{self.timestamp:.6f}:{self.value:.6f}"
    
    def to_dict(self) -> dict[str, Any]:
        """Convierte a diccionario para persistencia."""
        return {
            "sensor_id": self.sensor_id,
            "value": self.value,
            "timestamp": self.timestamp,
            "sensor_type": self.sensor_type,
            "sequence": self.sequence,
            "msg_id": self.get_msg_id(),
            "source": self.source,
            "device_timestamp": self.device_timestamp,
            "ingested_ts": self.ingested_ts,
        }
    
    def to_ingest_row(self) -> dict[str, Any]:
        """Convierte al formato esperado por el router/handler."""
        row = {
            "sensor_id": self.sensor_id,
            "value": self.value,
            "sensor_ts": self.timestamp,
            "ingested_ts": self.ingested_ts,
        }
        if self.device_timestamp:
            row["device_timestamp"] = self.device_timestamp
        elif self.timestamp:
            row["device_timestamp"] = datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
        if self.sequence is not None:
            row["sequence"] = self.sequence
        return row
    
    @classmethod
    def from_mqtt(
        cls,
        payload: "MQTTReadingPayload",
        ingested_ts: Optional[float] = None,
    ) -> "UnifiedReading":
        """Crea UnifiedReading desde payload MQTT.
        
        Args:
            payload: Payload MQTT validado
            ingested_ts: Timestamp de ingesta (default: ahora)
            
        Returns:
            UnifiedReading
            
        Raises:
            ValueError: Si sensor_id no es numérico
        """
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            raise ValueError(f"sensor_id must be numeric, got: {payload.sensor_id}")
        
        return cls(
            sensor_id=sensor_id,
            value=payload.value,
            timestamp=payload.timestamp_float,
            sensor_type=payload.sensor_type,
            sequence=payload.sequence,
            msg_id=payload.msg_id,
            source=ReadingSource.MQTT,
            device_timestamp=datetime.fromtimestamp(
                payload.timestamp_float, tz=timezone.utc
            ),
            ingested_ts=ingested_ts or time.time(),
        )
    
    @classmethod
    def from_http_packet(
        cls,
        reading: "SensorReadingUuidIn",
        sensor_id: int,
        device_ts: Optional[datetime] = None,
        ingested_ts: Optional[float] = None,
    ) -> "UnifiedReading":
        """Crea UnifiedReading desde lectura HTTP packet.
        
        Args:
            reading: Lectura del paquete HTTP
            sensor_id: ID del sensor resuelto
            device_ts: Timestamp del dispositivo (legacy)
            ingested_ts: Timestamp de ingesta (default: ahora)
            
        Returns:
            UnifiedReading
        """
        # Prioridad: sensor_ts (preciso) > device_ts (legacy) > ahora
        if reading.sensor_ts is not None:
            timestamp = reading.sensor_ts
            device_timestamp = datetime.fromtimestamp(reading.sensor_ts, tz=timezone.utc)
        elif device_ts is not None:
            timestamp = device_ts.timestamp()
            device_timestamp = device_ts
        else:
            timestamp = time.time()
            device_timestamp = None
        
        return cls(
            sensor_id=sensor_id,
            value=reading.value,
            timestamp=timestamp,
            sequence=reading.sequence,
            source=ReadingSource.HTTP_PACKET,
            device_timestamp=device_timestamp,
            ingested_ts=ingested_ts or time.time(),
        )
    
    @classmethod
    def from_http_legacy(
        cls,
        sensor_id: int,
        value: float,
        timestamp: Optional[datetime] = None,
        ingested_ts: Optional[float] = None,
    ) -> "UnifiedReading":
        """Crea UnifiedReading desde lectura HTTP legacy.
        
        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            timestamp: Timestamp de la lectura (opcional)
            ingested_ts: Timestamp de ingesta (default: ahora)
            
        Returns:
            UnifiedReading
        """
        if timestamp:
            ts_float = timestamp.timestamp()
            device_timestamp = timestamp
        else:
            ts_float = time.time()
            device_timestamp = None
        
        return cls(
            sensor_id=sensor_id,
            value=value,
            timestamp=ts_float,
            source=ReadingSource.HTTP_LEGACY,
            device_timestamp=device_timestamp,
            ingested_ts=ingested_ts or time.time(),
        )
    
    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UnifiedReading":
        """Crea UnifiedReading desde diccionario.
        
        Útil para deserializar desde Redis o BD.
        
        Args:
            data: Diccionario con datos de la lectura
            
        Returns:
            UnifiedReading
        """
        # Normalizar sensor_id
        sensor_id = data.get("sensor_id")
        if isinstance(sensor_id, str):
            sensor_id = int(sensor_id)
        
        # Normalizar timestamp
        timestamp = data.get("timestamp") or data.get("sensor_ts")
        if isinstance(timestamp, str):
            try:
                dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                timestamp = dt.timestamp()
            except Exception:
                timestamp = float(timestamp)
        elif isinstance(timestamp, datetime):
            timestamp = timestamp.timestamp()
        
        return cls(
            sensor_id=sensor_id,
            value=float(data.get("value", 0)),
            timestamp=float(timestamp or time.time()),
            sensor_type=data.get("sensor_type"),
            sequence=data.get("sequence"),
            msg_id=data.get("msg_id"),
            source=ReadingSource(data.get("source", "unknown")),
            ingested_ts=float(data.get("ingested_ts", time.time())),
        )

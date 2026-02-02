"""Dead Letter Queue para mensajes fallidos.

Almacena mensajes que no pudieron procesarse para análisis posterior.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass
class DLQEntry:
    """Entrada en la Dead Letter Queue."""
    payload: dict
    error: str
    error_type: str
    source: str
    timestamp: float
    sensor_id: Optional[int] = None
    msg_id: Optional[str] = None


class DeadLetterQueue:
    """Dead Letter Queue usando Redis Streams.
    
    Almacena mensajes fallidos con metadata para debugging y reprocesamiento.
    
    Attributes:
        stream_name: Nombre del stream en Redis
        max_len: Máximo de entradas (aproximado, usa MAXLEN ~)
    """
    
    STREAM_NAME = "dlq:ingest"
    DEFAULT_MAX_LEN = 10000
    
    def __init__(
        self,
        redis_client: Optional["redis.Redis"] = None,
        stream_name: str = STREAM_NAME,
        max_len: int = DEFAULT_MAX_LEN,
    ):
        """Inicializa la DLQ.
        
        Args:
            redis_client: Cliente Redis. Si es None, DLQ deshabilitada.
            stream_name: Nombre del stream en Redis.
            max_len: Máximo de entradas en el stream.
        """
        self._redis = redis_client
        self._stream = stream_name
        self._max_len = max_len
        self._enabled = redis_client is not None and REDIS_AVAILABLE
        
        # Stats
        self._total_sent = 0
        self._send_errors = 0
    
    @property
    def enabled(self) -> bool:
        """Indica si la DLQ está habilitada."""
        return self._enabled
    
    @property
    def stats(self) -> dict:
        """Estadísticas de la DLQ."""
        return {
            "enabled": self._enabled,
            "stream_name": self._stream,
            "total_sent": self._total_sent,
            "send_errors": self._send_errors,
        }
    
    def send(
        self,
        payload: Any,
        error: str,
        error_type: str,
        source: str,
        sensor_id: Optional[int] = None,
        msg_id: Optional[str] = None,
    ) -> bool:
        """Envía un mensaje a la DLQ.
        
        Args:
            payload: Payload original (será serializado a JSON)
            error: Mensaje de error
            error_type: Tipo de error (ej: "parse_error", "validation_error")
            source: Origen del mensaje (ej: "mqtt", "http")
            sensor_id: ID del sensor (si se conoce)
            msg_id: ID del mensaje (si se conoce)
            
        Returns:
            True si se envió correctamente, False si falló o DLQ deshabilitada.
        """
        if not self._enabled:
            # Log local si DLQ no está disponible
            logger.warning(
                "DLQ_DISABLED payload=%s error=%s source=%s",
                str(payload)[:200], error, source
            )
            return False
        
        try:
            # Serializar payload
            if isinstance(payload, bytes):
                payload_str = payload.decode("utf-8", errors="replace")
            elif isinstance(payload, dict):
                payload_str = json.dumps(payload, default=str)
            else:
                payload_str = str(payload)
            
            # Construir entrada
            entry = {
                "payload": payload_str[:5000],  # Limitar tamaño
                "error": str(error)[:1000],
                "error_type": error_type,
                "source": source,
                "timestamp": str(time.time()),
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            
            if sensor_id is not None:
                entry["sensor_id"] = str(sensor_id)
            if msg_id is not None:
                entry["msg_id"] = msg_id
            
            # Enviar a Redis Stream
            self._redis.xadd(
                self._stream,
                entry,
                maxlen=self._max_len,
                approximate=True,
            )
            
            self._total_sent += 1
            logger.info(
                "DLQ_SENT source=%s error_type=%s sensor_id=%s",
                source, error_type, sensor_id
            )
            return True
            
        except Exception as e:
            self._send_errors += 1
            logger.error("DLQ_SEND_ERROR err=%s payload=%s", e, str(payload)[:100])
            return False
    
    def get_pending_count(self) -> int:
        """Obtiene el número de mensajes pendientes en la DLQ."""
        if not self._enabled:
            return 0
        
        try:
            info = self._redis.xinfo_stream(self._stream)
            return info.get("length", 0)
        except Exception:
            return 0
    
    def get_recent(self, count: int = 10) -> list[dict]:
        """Obtiene los mensajes más recientes de la DLQ.
        
        Args:
            count: Número de mensajes a obtener
            
        Returns:
            Lista de mensajes (más reciente primero)
        """
        if not self._enabled:
            return []
        
        try:
            # XREVRANGE para obtener los más recientes primero
            entries = self._redis.xrevrange(self._stream, count=count)
            return [
                {"id": entry_id.decode() if isinstance(entry_id, bytes) else entry_id, 
                 **{k.decode() if isinstance(k, bytes) else k: 
                    v.decode() if isinstance(v, bytes) else v 
                    for k, v in data.items()}}
                for entry_id, data in entries
            ]
        except Exception as e:
            logger.error("DLQ_READ_ERROR err=%s", e)
            return []

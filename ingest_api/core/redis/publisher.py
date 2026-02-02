"""Publicador a Redis Streams."""

from __future__ import annotations

import logging
from typing import Optional

from .connection import RedisConnection
from ..domain.reading import Reading

logger = logging.getLogger(__name__)

DEFAULT_STREAM = "readings:validated"
DEFAULT_MAX_LEN = 10000


class RedisPublisher:
    """Publica lecturas a Redis Streams para ML.
    
    Responsabilidades:
    - Publicar lecturas validadas a stream
    - Gestionar backpressure (maxlen)
    """
    
    def __init__(
        self,
        connection: RedisConnection,
        stream_name: str = DEFAULT_STREAM,
        max_len: int = DEFAULT_MAX_LEN,
    ):
        self._conn = connection
        self._stream = stream_name
        self._max_len = max_len
    
    def publish(self, reading: Reading) -> bool:
        """Publica una lectura al stream.
        
        Args:
            reading: Lectura procesada
            
        Returns:
            True si se publicó correctamente
        """
        if not self._conn.is_connected:
            return False
        
        try:
            data = reading.to_redis_data()
            self._conn.client.xadd(
                self._stream,
                data,
                maxlen=self._max_len,
                approximate=True,
            )
            
            logger.debug(
                "[REDIS] Published: sensor_id=%d value=%.4f",
                reading.sensor_id,
                reading.value,
            )
            return True
            
        except Exception as e:
            logger.warning("[REDIS] Publish failed: %s", e)
            return False
    
    @property
    def stream_name(self) -> str:
        return self._stream

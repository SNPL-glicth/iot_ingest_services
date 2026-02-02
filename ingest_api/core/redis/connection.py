"""Conexión a Redis."""

from __future__ import annotations

import logging
import os
from typing import Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class RedisConnection:
    """Gestiona la conexión a Redis."""
    
    def __init__(self, url: Optional[str] = None):
        self._url = url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self._client: Optional[redis.Redis] = None
        self._connected = False
    
    @property
    def client(self) -> Optional[redis.Redis]:
        return self._client
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> bool:
        """Conecta a Redis."""
        if not REDIS_AVAILABLE:
            logger.warning("[REDIS] redis-py not available")
            return False
        
        try:
            self._client = redis.Redis.from_url(
                self._url,
                decode_responses=False,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            self._client.ping()
            self._connected = True
            logger.info("[REDIS] Connected: %s", self._url.split("@")[-1])
            return True
        except Exception as e:
            self._connected = False
            logger.warning("[REDIS] Connection failed: %s", e)
            return False
    
    def disconnect(self):
        """Desconecta de Redis."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
        self._connected = False

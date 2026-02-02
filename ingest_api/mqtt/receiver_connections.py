"""Connection management for MQTT receiver.

Extracted from receiver.py for modularity.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Manages database connection for MQTT receiver."""
    
    def __init__(self):
        self._engine = None
        self._connected = False
    
    def connect(self) -> bool:
        """Connect to database."""
        try:
            from sqlalchemy import create_engine
            
            db_url = os.getenv("DATABASE_URL")
            if not db_url:
                logger.error("[DB] DATABASE_URL not set")
                return False
            
            self._engine = create_engine(db_url, pool_pre_ping=True)
            self._connected = True
            logger.info("[DB] Connected successfully")
            return True
            
        except Exception as e:
            logger.exception("[DB] Connection failed: %s", e)
            return False
    
    @property
    def engine(self):
        return self._engine
    
    @property
    def is_connected(self) -> bool:
        return self._connected


class RedisConnection:
    """Manages Redis connection for MQTT receiver."""
    
    def __init__(self):
        self._client = None
        self._connected = False
        self._stream_name = "readings:raw"
    
    def connect(self) -> bool:
        """Connect to Redis."""
        try:
            import redis
            
            redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
            self._client = redis.from_url(redis_url)
            self._client.ping()
            self._connected = True
            logger.info("[Redis] Connected to %s", redis_url)
            return True
            
        except Exception as e:
            logger.warning("[Redis] Connection failed (optional): %s", e)
            return False
    
    @property
    def client(self):
        return self._client
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def stream_name(self) -> str:
        return self._stream_name

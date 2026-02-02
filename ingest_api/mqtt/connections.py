"""Módulo de conexiones para el receptor MQTT.

Maneja conexiones a:
- Base de datos (SQL Server)
- Redis (para ML streams)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Gestiona la conexión a la base de datos."""
    
    def __init__(self):
        self._engine: Optional[Engine] = None
    
    @property
    def engine(self) -> Optional[Engine]:
        return self._engine
    
    @property
    def is_connected(self) -> bool:
        return self._engine is not None
    
    def connect(self) -> bool:
        """Conecta a la BD."""
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "1433")
            db_name = os.getenv("DB_NAME", "iot")
            db_user = os.getenv("DB_USER", "sa")
            db_pass = os.getenv("DB_PASSWORD", "")
            db_url = f"mssql+pymssql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        try:
            self._engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
            )
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("[DB] Connected to database")
            return True
        except Exception as e:
            self._engine = None
            logger.error("[DB] Connection failed: %s", e)
            return False


class RedisConnection:
    """Gestiona la conexión a Redis para ML streams."""
    
    def __init__(self, stream_name: str = "readings:validated", max_len: int = 10000):
        self._client: Optional[redis.Redis] = None
        self._connected = False
        self._stream_name = stream_name
        self._max_len = max_len
    
    @property
    def client(self) -> Optional[redis.Redis]:
        return self._client
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def stream_name(self) -> str:
        return self._stream_name
    
    def connect(self) -> bool:
        """Conecta a Redis."""
        if not REDIS_AVAILABLE:
            logger.warning("[REDIS] redis-py not available - ML stream disabled")
            return False
        
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        try:
            self._client = redis.Redis.from_url(
                redis_url,
                decode_responses=False,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            self._client.ping()
            self._connected = True
            logger.info("[REDIS] Connected: %s", redis_url.split("@")[-1])
            return True
        except Exception as e:
            self._connected = False
            logger.warning("[REDIS] Connection failed: %s", e)
            return False
    
    def publish(self, data: dict) -> bool:
        """Publica datos al stream."""
        if not self._connected or not self._client:
            return False
        
        try:
            self._client.xadd(
                self._stream_name,
                data,
                maxlen=self._max_len,
                approximate=True,
            )
            return True
        except Exception as e:
            logger.warning("[REDIS] Publish failed: %s", e)
            return False

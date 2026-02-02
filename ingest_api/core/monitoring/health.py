"""Health checks del sistema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlalchemy.engine import Engine
from sqlalchemy import text

from ..redis.connection import RedisConnection


@dataclass
class HealthStatus:
    """Estado de salud del sistema."""
    healthy: bool
    mqtt_connected: bool
    db_connected: bool
    redis_connected: bool
    messages_processed: int
    messages_failed: int
    sp_name: str = "sp_insert_reading_and_check_threshold"
    
    def to_dict(self) -> dict:
        return {
            "healthy": self.healthy,
            "mqtt_connected": self.mqtt_connected,
            "db_connected": self.db_connected,
            "redis_connected": self.redis_connected,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "uses_sp": self.sp_name,
        }


class HealthChecker:
    """Verifica el estado de salud del sistema."""
    
    def __init__(
        self,
        engine: Optional[Engine] = None,
        redis_conn: Optional[RedisConnection] = None,
    ):
        self._engine = engine
        self._redis = redis_conn
    
    def check_database(self) -> bool:
        """Verifica conexión a BD."""
        if not self._engine:
            return False
        
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
    
    def check_redis(self) -> bool:
        """Verifica conexión a Redis."""
        if not self._redis:
            return False
        return self._redis.is_connected
    
    def get_status(
        self,
        mqtt_connected: bool,
        processed: int,
        failed: int,
    ) -> HealthStatus:
        """Obtiene estado de salud completo."""
        db_ok = self.check_database()
        redis_ok = self.check_redis()
        
        return HealthStatus(
            healthy=mqtt_connected and db_ok,
            mqtt_connected=mqtt_connected,
            db_connected=db_ok,
            redis_connected=redis_ok,
            messages_processed=processed,
            messages_failed=failed,
        )

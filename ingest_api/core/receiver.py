"""Receptor MQTT modular - Punto de entrada principal.

Usa la arquitectura modular:
- transport/   → Cliente MQTT
- adapters/    → Conversión MQTT → Dominio
- pipeline/    → Procesamiento con SP
- redis/       → Publicación a ML
- monitoring/  → Stats y health
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .transport.mqtt_client import MQTTClient
from .transport.message_handler import MessageHandler
from .adapters.mqtt_adapter import MQTTAdapter
from .pipeline.processor import ReadingProcessor
from .redis.connection import RedisConnection
from .redis.publisher import RedisPublisher
from .monitoring.health import HealthChecker
from .monitoring.stats import Stats

logger = logging.getLogger(__name__)


class ModularReceiver:
    """Receptor MQTT con arquitectura modular.
    
    Componentes:
    - MQTTClient: Conexión y suscripción MQTT
    - MessageHandler: Parseo y delegación
    - MQTTAdapter: Conversión de contrato
    - ReadingProcessor: Pipeline de procesamiento
    - RedisPublisher: Publicación a ML (opcional)
    """
    
    def __init__(self):
        self._mqtt: Optional[MQTTClient] = None
        self._handler: Optional[MessageHandler] = None
        self._engine: Optional[Engine] = None
        self._redis: Optional[RedisConnection] = None
        self._health: Optional[HealthChecker] = None
        self._running = False
    
    def start(self) -> bool:
        """Inicia el receptor."""
        try:
            # 1. Conectar a BD
            self._engine = self._create_engine()
            if not self._engine:
                logger.error("[RECEIVER] Database connection failed")
                return False
            
            # 2. Conectar a Redis (opcional)
            self._redis = RedisConnection()
            self._redis.connect()
            
            # 3. Crear pipeline
            redis_pub = RedisPublisher(self._redis) if self._redis.is_connected else None
            processor = ReadingProcessor(self._engine, redis_pub)
            
            # 4. Crear handler
            self._handler = MessageHandler(processor, MQTTAdapter())
            
            # 5. Crear cliente MQTT
            self._mqtt = MQTTClient(
                broker_host=os.getenv("MQTT_BROKER_HOST", "localhost"),
                broker_port=int(os.getenv("MQTT_BROKER_PORT", "1883")),
                username=os.getenv("MQTT_USERNAME"),
                password=os.getenv("MQTT_PASSWORD"),
            )
            self._mqtt.set_message_handler(self._handler.handle)
            
            # 6. Conectar MQTT
            if not self._mqtt.connect():
                logger.error("[RECEIVER] MQTT connection failed")
                return False
            
            # 7. Health checker
            self._health = HealthChecker(self._engine, self._redis)
            
            self._running = True
            logger.info("[RECEIVER] Started successfully")
            return True
            
        except Exception as e:
            logger.exception("[RECEIVER] Start failed: %s", e)
            return False
    
    def stop(self):
        """Detiene el receptor."""
        self._running = False
        
        if self._mqtt:
            self._mqtt.disconnect()
        
        if self._handler:
            logger.info("[RECEIVER] Stopped. %s", self._handler.stats)
    
    def _create_engine(self) -> Optional[Engine]:
        """Crea engine de BD."""
        db_url = os.getenv("DATABASE_URL")
        if not db_url:
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "1433")
            db_name = os.getenv("DB_NAME", "iot")
            db_user = os.getenv("DB_USER", "sa")
            db_pass = os.getenv("DB_PASSWORD", "")
            db_url = f"mssql+pymssql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        
        try:
            engine = create_engine(
                db_url,
                pool_size=5,
                max_overflow=10,
                pool_pre_ping=True,
                pool_recycle=300,
            )
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("[RECEIVER] Database connected")
            return engine
        except Exception as e:
            logger.error("[RECEIVER] Database error: %s", e)
            return None
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def is_connected(self) -> bool:
        return self._mqtt.is_connected if self._mqtt else False
    
    @property
    def stats(self) -> dict:
        """Estadísticas del receptor."""
        handler_stats = self._handler.stats if self._handler else Stats()
        return {
            "running": self._running,
            "connected": self.is_connected,
            "db_connected": self._engine is not None,
            "redis_connected": self._redis.is_connected if self._redis else False,
            **handler_stats.to_dict(),
            "uses_sp": True,
        }
    
    def health_check(self) -> dict:
        """Health check del receptor."""
        if not self._health or not self._handler:
            return {"healthy": False, "reason": "Not initialized"}
        
        status = self._health.get_status(
            mqtt_connected=self.is_connected,
            processed=self._handler.stats.processed,
            failed=self._handler.stats.failed,
        )
        return status.to_dict()


# Singleton
_receiver: Optional[ModularReceiver] = None


def get_modular_receiver() -> Optional[ModularReceiver]:
    """Obtiene el receptor modular singleton."""
    return _receiver


def start_modular_receiver() -> bool:
    """Inicia el receptor modular."""
    global _receiver
    
    if _receiver is not None:
        return _receiver.is_running
    
    _receiver = ModularReceiver()
    return _receiver.start()


def stop_modular_receiver():
    """Detiene el receptor modular."""
    global _receiver
    
    if _receiver is not None:
        _receiver.stop()
        _receiver = None

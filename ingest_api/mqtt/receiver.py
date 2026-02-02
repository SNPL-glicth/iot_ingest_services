"""Receptor MQTT principal.

Refactored 2026-02-02: Split into modular files.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Optional

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    import json
    ORJSON_AVAILABLE = False

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False

from .validators import validate_mqtt_reading
from .receiver_connections import DatabaseConnection, RedisConnection
from .receiver_stats import ReceiverStats
from .processor import ReadingProcessor
from ..pipelines.resilience import get_deduplicator, get_dlq

logger = logging.getLogger(__name__)


class MQTTReceiver:
    """Receptor MQTT que procesa lecturas con el SP de dominio."""
    
    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: str = "ingest-mqtt-receiver",
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.client_id = f"{client_id}-{int(time.time())}"
        
        self._client: Optional[mqtt.Client] = None
        self._running = False
        self._connected = False
        
        self._db = DatabaseConnection()
        self._redis = RedisConnection()
        self._processor: Optional[ReadingProcessor] = None
        self._stats = ReceiverStats()
    
    def start(self) -> bool:
        """Inicia el receptor MQTT."""
        if not PAHO_AVAILABLE:
            logger.error("[MQTT] paho-mqtt not available")
            return False
        
        try:
            # Conectar a BD
            if not self._db.connect():
                logger.error("[MQTT] Database connection failed")
                return False
            
            # Conectar a Redis (opcional)
            self._redis.connect()
            
            # Crear procesador
            self._processor = ReadingProcessor(self._db.engine, self._redis)
            
            # Crear cliente MQTT
            self._client = mqtt.Client(
                client_id=self.client_id,
                protocol=mqtt.MQTTv311,
                callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            )
            
            self._client.on_connect = self._on_connect
            self._client.on_disconnect = self._on_disconnect
            self._client.on_message = self._on_message
            
            if self.username and self.password:
                self._client.username_pw_set(self.username, self.password)
            
            logger.info("[MQTT] Connecting to %s:%d", self.broker_host, self.broker_port)
            
            self._client.connect(self.broker_host, self.broker_port, keepalive=60)
            self._client.loop_start()
            self._running = True
            
            # Esperar conexión
            for _ in range(50):
                if self._connected:
                    break
                time.sleep(0.1)
            
            if self._connected:
                logger.info("[MQTT] Started successfully")
                return True
            else:
                logger.error("[MQTT] Connection timeout")
                return False
            
        except Exception as e:
            logger.exception("[MQTT] Start failed: %s", e)
            return False
    
    def stop(self):
        """Detiene el receptor."""
        self._running = False
        
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.warning("[MQTT] Error stopping: %s", e)
        
        logger.info("[MQTT] Stopped. %s", self._stats)
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback de conexión."""
        if rc == 0:
            self._connected = True
            logger.info("[MQTT] Connected to broker")
            
            topic = "iot/sensors/+/readings"
            client.subscribe(topic, qos=1)
            logger.info("[MQTT] Subscribed to %s", topic)
        else:
            self._connected = False
            logger.error("[MQTT] Connection failed: rc=%d", rc)
    
    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        """Callback de desconexión."""
        self._connected = False
        logger.warning("[MQTT] Disconnected (rc=%d)", rc)
    
    def _on_message(self, client, userdata, msg):
        """Callback de mensaje recibido."""
        from .message_handler import handle_message
        handle_message(msg, self._stats, self._processor)
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    @property
    def stats(self) -> dict:
        return {
            "running": self._running,
            "connected": self._connected,
            "broker": f"{self.broker_host}:{self.broker_port}",
            "messages_received": self._stats.received,
            "messages_processed": self._stats.processed,
            "messages_failed": self._stats.failed,
            "last_message_at": self._stats.last_message_at,
            "db_connected": self._db.is_connected,
            "redis_connected": self._redis.is_connected,
            "redis_stream": self._redis.stream_name,
            "uses_sp": True,
        }
    
    def health_check(self) -> dict:
        return {
            "healthy": self._running and self._connected and self._db.is_connected,
            "running": self._running,
            "connected": self._connected,
            "db_connected": self._db.is_connected,
            "redis_connected": self._redis.is_connected,
            "messages_processed": self._stats.processed,
            "messages_failed": self._stats.failed,
            "uses_sp": "sp_insert_reading_and_check_threshold",
        }


# Re-export singleton functions from receiver_singleton
from .receiver_singleton import get_receiver, start_receiver, stop_receiver

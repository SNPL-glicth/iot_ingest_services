"""Receptor MQTT principal.

Refactored 2026-02-02: Split into modular files.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False

from .receiver_connections import DatabaseConnection, RedisConnection
from .receiver_stats import ReceiverStats
from .processor import ReadingProcessor

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
        self._async_processor = None
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
            
            # Crear procesador (with optional circuit breaker + DLQ)
            cb, dlq = self._create_resilience()
            self._processor = ReadingProcessor(
                self._db.engine, self._redis,
                circuit_breaker=cb, dlq=dlq,
            )
            
            # Crear async processor (feature-flag controlled)
            from .async_processor import create_async_processor
            self._async_processor = create_async_processor(self._processor)
            
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
        
        if self._async_processor is not None:
            self._async_processor.stop(drain=True)
        
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
            
            # IoT topic - ALWAYS subscribed (legacy, must not change)
            iot_topic = "iot/sensors/+/readings"
            client.subscribe(iot_topic, qos=1)
            logger.info("[MQTT] Subscribed to %s (IoT legacy)", iot_topic)
            
            # Multi-domain topics - only if feature flag enabled
            import os
            if os.getenv("FF_MQTT_MULTI_DOMAIN", "false").lower() in ("true", "1", "yes", "on"):
                multi_domain_topics = [
                    ("infrastructure/+/+/data", 1),
                    ("finance/+/+/data", 1),
                    ("health/+/+/data", 1),
                ]
                for topic, qos in multi_domain_topics:
                    client.subscribe(topic, qos=qos)
                    logger.info("[MQTT] Subscribed to %s (multi-domain)", topic)
            else:
                logger.info("[MQTT] Multi-domain topics disabled (FF_MQTT_MULTI_DOMAIN=false)")
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
        handle_message(msg, self._stats, self._processor, self._async_processor)
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def _create_resilience(self):
        """Create circuit breaker + DLQ if feature flag enabled."""
        import os
        enabled = os.getenv("ML_INGEST_CIRCUIT_BREAKER_ENABLED", "true").lower() in (
            "true", "1", "yes", "on",
        )
        if not enabled:
            return None, None
        try:
            from ..pipelines.resilience.circuit_breaker import CircuitBreaker
            from ..pipelines.resilience.circuit_breaker_config import CircuitBreakerConfig
            from ..pipelines.resilience.dead_letter import DeadLetterQueue
            threshold = int(os.getenv("ML_INGEST_CB_FAILURE_THRESHOLD", "5"))
            timeout = float(os.getenv("ML_INGEST_CB_TIMEOUT_SECONDS", "30"))
            cfg = CircuitBreakerConfig(
                failure_threshold=threshold, recovery_timeout_seconds=timeout,
            )
            cb = CircuitBreaker("sql_sp", config=cfg)
            redis_client = self._redis.client if self._redis.is_connected else None
            dlq = DeadLetterQueue(redis_client=redis_client)
            logger.info("[MQTT] Circuit breaker enabled (threshold=%d, timeout=%.0fs)",
                        threshold, timeout)
            return cb, dlq
        except Exception as exc:
            logger.warning("[MQTT] Circuit breaker init failed: %s", exc)
            return None, None

    @property
    def stats(self) -> dict:
        s = self._stats
        return {
            "running": self._running, "connected": self._connected,
            "broker": f"{self.broker_host}:{self.broker_port}",
            "received": s.received, "processed": s.processed,
            "failed": s.failed, "last_message_at": s.last_message_at,
            "db": self._db.is_connected, "redis": self._redis.is_connected,
            "async": self._async_processor is not None,
        }

    def health_check(self) -> dict:
        return {
            "healthy": self._running and self._connected and self._db.is_connected,
            "running": self._running, "connected": self._connected,
            "db": self._db.is_connected, "redis": self._redis.is_connected,
            "processed": self._stats.processed, "failed": self._stats.failed,
            "async": self._async_processor is not None,
        }

# Re-export singleton functions from receiver_singleton
from .receiver_singleton import get_receiver, start_receiver, stop_receiver

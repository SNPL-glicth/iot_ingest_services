"""Receptor MQTT simple usando paho-mqtt directamente.

Este receptor NO depende de iot_mqtt module.
Escribe a BD usando BatchInserter Y publica a Redis para ML.

Flujo:
  MQTT topic iot/sensors/{id}/readings
  → simple_receiver (este archivo)
  → BatchInserter → BD (sensor_readings)
  → Redis Stream readings:validated → ML
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from datetime import datetime
from typing import Optional, Any

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .validators import validate_mqtt_reading, MQTTReadingPayload

logger = logging.getLogger(__name__)


class SimpleMQTTReceiver:
    """Receptor MQTT simple que escribe a BD y publica a Redis para ML.
    
    NO depende de iot_mqtt module.
    USA paho-mqtt directamente.
    ESCRIBE a BD via BatchInserter.
    PUBLICA a Redis Stream para ML.
    """
    
    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: str = "ingest-simple-receiver",
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.client_id = f"{client_id}-{int(time.time())}"
        
        self._client: Optional[mqtt.Client] = None
        self._running = False
        self._connected = False
        self._loop_thread: Optional[threading.Thread] = None
        
        # Stats
        self._messages_received = 0
        self._messages_processed = 0
        self._messages_failed = 0
        self._last_message_at: float = 0
        
        # BatchInserter reference
        self._batch_inserter = None
        
        # Redis client for ML publishing
        self._redis: Optional[redis.Redis] = None
        self._redis_connected = False
        self._redis_stream = "readings:validated"
        self._redis_max_len = 10000
    
    def start(self) -> bool:
        """Inicia el receptor MQTT."""
        if not PAHO_AVAILABLE:
            logger.error("[SIMPLE_MQTT] paho-mqtt not available")
            return False
        
        try:
            # Obtener BatchInserter
            from ..batch_inserter import get_batch_inserter
            self._batch_inserter = get_batch_inserter()
            
            if self._batch_inserter is None:
                logger.error("[SIMPLE_MQTT] BatchInserter not initialized")
                return False
            
            # Conectar a Redis para ML
            self._connect_redis()
            
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
            
            # Conectar
            logger.info(
                "[SIMPLE_MQTT] Connecting to %s:%d",
                self.broker_host,
                self.broker_port,
            )
            
            self._client.connect(self.broker_host, self.broker_port, keepalive=60)
            self._client.loop_start()
            self._running = True
            
            # Esperar conexión
            for _ in range(50):
                if self._connected:
                    break
                time.sleep(0.1)
            
            if self._connected:
                logger.info("[SIMPLE_MQTT] Started successfully")
                return True
            else:
                logger.error("[SIMPLE_MQTT] Connection timeout")
                return False
            
        except Exception as e:
            logger.exception("[SIMPLE_MQTT] Start failed: %s", e)
            return False
    
    def stop(self):
        """Detiene el receptor."""
        self._running = False
        
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.warning("[SIMPLE_MQTT] Error stopping: %s", e)
        
        logger.info(
            "[SIMPLE_MQTT] Stopped. Stats: received=%d processed=%d failed=%d",
            self._messages_received,
            self._messages_processed,
            self._messages_failed,
        )
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback de conexión."""
        if rc == 0:
            self._connected = True
            logger.info("[SIMPLE_MQTT] Connected to MQTT broker")
            
            # Suscribirse al topic
            topic = "iot/sensors/+/readings"
            client.subscribe(topic, qos=1)
            logger.info("[SIMPLE_MQTT] Subscribed to %s", topic)
        else:
            self._connected = False
            logger.error("[SIMPLE_MQTT] Connection failed: rc=%d", rc)
    
    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        """Callback de desconexión."""
        self._connected = False
        logger.warning("[SIMPLE_MQTT] Disconnected (rc=%d)", rc)
    
    def _on_message(self, client, userdata, msg):
        """Callback de mensaje recibido."""
        self._messages_received += 1
        self._last_message_at = time.time()
        
        try:
            # Parsear JSON
            try:
                data = json.loads(msg.payload.decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.warning(
                    "[SIMPLE_MQTT] Invalid JSON: %s (topic=%s)",
                    e,
                    msg.topic,
                )
                self._messages_failed += 1
                return
            
            # Log del mensaje recibido
            logger.debug(
                "[SIMPLE_MQTT] Received: topic=%s payload=%s",
                msg.topic,
                data,
            )
            
            # Validar payload
            validation = validate_mqtt_reading(data)
            
            if not validation.valid:
                logger.warning(
                    "[SIMPLE_MQTT] Validation failed: %s (topic=%s)",
                    validation.error,
                    msg.topic,
                )
                self._messages_failed += 1
                return
            
            # Insertar en BD via BatchInserter
            self._insert_reading(validation.payload)
            self._messages_processed += 1
            
            # Log periódico
            if self._messages_processed % 10 == 0:
                logger.info(
                    "[SIMPLE_MQTT] Stats: received=%d processed=%d failed=%d",
                    self._messages_received,
                    self._messages_processed,
                    self._messages_failed,
                )
            
        except Exception as e:
            logger.exception("[SIMPLE_MQTT] Processing error: %s", e)
            self._messages_failed += 1
    
    def _connect_redis(self):
        """Conecta a Redis para publicar a ML."""
        if not REDIS_AVAILABLE:
            logger.warning("[SIMPLE_MQTT] Redis not available - ML will not receive readings")
            return
        
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        try:
            self._redis = redis.Redis.from_url(
                redis_url,
                decode_responses=False,
                socket_timeout=5.0,
                socket_connect_timeout=5.0,
            )
            self._redis.ping()
            self._redis_connected = True
            logger.info("[SIMPLE_MQTT] Connected to Redis for ML: %s", redis_url.split("@")[-1])
        except Exception as e:
            self._redis_connected = False
            logger.warning("[SIMPLE_MQTT] Redis connection failed: %s - ML will not receive readings", e)

    def _publish_to_ml(self, payload: MQTTReadingPayload):
        """Publica lectura a Redis Stream para ML."""
        if not self._redis_connected or not self._redis:
            return
        
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            return
        
        # Obtener sensor_type de metadata o usar "unknown"
        sensor_type = payload.sensor_type or "unknown"
        
        # Timestamp como epoch float
        timestamp_float = payload.timestamp_float
        
        try:
            data = {
                "sensor_id": str(sensor_id),
                "sensor_type": sensor_type,
                "value": str(payload.value),
                "timestamp": str(timestamp_float),
            }
            
            self._redis.xadd(
                self._redis_stream,
                data,
                maxlen=self._redis_max_len,
                approximate=True,
            )
            
            logger.debug(
                "[SIMPLE_MQTT] Published to Redis ML: sensor_id=%d value=%.4f",
                sensor_id,
                payload.value,
            )
        except Exception as e:
            logger.warning("[SIMPLE_MQTT] Redis publish failed: %s", e)

    def _insert_reading(self, payload: MQTTReadingPayload):
        """Inserta lectura en BD via BatchInserter Y publica a Redis para ML."""
        if self._batch_inserter is None:
            logger.error("[SIMPLE_MQTT] BatchInserter not available")
            return
        
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            logger.warning("[SIMPLE_MQTT] Invalid sensor_id: %s", payload.sensor_id)
            return
        
        # Parsear timestamp
        try:
            device_timestamp = datetime.fromisoformat(
                payload.timestamp.replace("Z", "+00:00")
            )
        except Exception:
            device_timestamp = None
        
        # 1. Insertar via BatchInserter → BD
        try:
            added = self._batch_inserter.add(
                sensor_id=sensor_id,
                value=payload.value,
                device_timestamp=device_timestamp,
            )
            if added:
                logger.debug(
                    "[SIMPLE_MQTT] Queued BD: sensor_id=%d value=%.4f",
                    sensor_id,
                    payload.value,
                )
            else:
                logger.warning(
                    "[SIMPLE_MQTT] Dropped (backpressure): sensor_id=%d",
                    sensor_id,
                )
        except Exception as e:
            logger.error("[SIMPLE_MQTT] BD Insert failed: %s", e)
            raise
        
        # 2. Publicar a Redis → ML
        self._publish_to_ml(payload)
    
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
            "messages_received": self._messages_received,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
            "last_message_at": self._last_message_at,
            "redis_connected": self._redis_connected,
            "redis_stream": self._redis_stream,
        }
    
    def health_check(self) -> dict:
        return {
            "healthy": self._running and self._connected,
            "running": self._running,
            "connected": self._connected,
            "redis_connected": self._redis_connected,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
        }


# Singleton
_simple_receiver: Optional[SimpleMQTTReceiver] = None


def get_simple_receiver() -> Optional[SimpleMQTTReceiver]:
    """Obtiene el receptor simple singleton."""
    return _simple_receiver


def start_simple_receiver() -> bool:
    """Inicia el receptor simple."""
    global _simple_receiver
    
    if _simple_receiver is not None:
        return _simple_receiver.is_running
    
    broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
    broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
    username = os.getenv("MQTT_USERNAME") or None
    password = os.getenv("MQTT_PASSWORD") or None
    
    _simple_receiver = SimpleMQTTReceiver(
        broker_host=broker_host,
        broker_port=broker_port,
        username=username,
        password=password,
    )
    
    return _simple_receiver.start()


def stop_simple_receiver():
    """Detiene el receptor simple."""
    global _simple_receiver
    
    if _simple_receiver is not None:
        _simple_receiver.stop()
        _simple_receiver = None

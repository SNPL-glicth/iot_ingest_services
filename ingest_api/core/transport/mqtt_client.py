"""Cliente MQTT para recepción de lecturas."""

from __future__ import annotations

import logging
import os
import time
from typing import Optional, Callable

try:
    import paho.mqtt.client as mqtt
    PAHO_AVAILABLE = True
except ImportError:
    PAHO_AVAILABLE = False

logger = logging.getLogger(__name__)


class MQTTClient:
    """Cliente MQTT ligero para recepción de lecturas.
    
    Responsabilidades:
    - Conexión/desconexión a broker MQTT
    - Suscripción a topics
    - Delegación de mensajes a handler
    """
    
    def __init__(
        self,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: str = "ingest-mqtt",
    ):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.username = username
        self.password = password
        self.client_id = f"{client_id}-{int(time.time())}"
        
        self._client: Optional[mqtt.Client] = None
        self._connected = False
        self._message_handler: Optional[Callable] = None
    
    def set_message_handler(self, handler: Callable):
        """Configura el handler de mensajes."""
        self._message_handler = handler
    
    def connect(self) -> bool:
        """Conecta al broker MQTT."""
        if not PAHO_AVAILABLE:
            logger.error("[MQTT] paho-mqtt not available")
            return False
        
        try:
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
            
            # Esperar conexión
            for _ in range(50):
                if self._connected:
                    return True
                time.sleep(0.1)
            
            logger.error("[MQTT] Connection timeout")
            return False
            
        except Exception as e:
            logger.exception("[MQTT] Connection failed: %s", e)
            return False
    
    def disconnect(self):
        """Desconecta del broker."""
        if self._client:
            try:
                self._client.loop_stop()
                self._client.disconnect()
            except Exception as e:
                logger.warning("[MQTT] Disconnect error: %s", e)
        self._connected = False
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback de conexión."""
        if rc == 0:
            self._connected = True
            logger.info("[MQTT] Connected to broker")
            client.subscribe("iot/sensors/+/readings", qos=1)
            logger.info("[MQTT] Subscribed to iot/sensors/+/readings")
        else:
            self._connected = False
            logger.error("[MQTT] Connection failed: rc=%d", rc)
    
    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        """Callback de desconexión."""
        self._connected = False
        logger.warning("[MQTT] Disconnected (rc=%d)", rc)
    
    def _on_message(self, client, userdata, msg):
        """Callback de mensaje - delega al handler."""
        if self._message_handler:
            self._message_handler(msg.topic, msg.payload)
    
    @property
    def is_connected(self) -> bool:
        return self._connected

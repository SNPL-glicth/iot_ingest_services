"""Receptor MQTT para ingesta de lecturas.

Escucha mensajes MQTT y los procesa a través del pipeline de ingesta.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from .validators import validate_mqtt_reading, MQTTReadingPayload

logger = logging.getLogger(__name__)

try:
    import sys
    sys.path.insert(0, str(__file__).replace("iot_ingest_services", "").rstrip("/\\"))
    from iot_mqtt import MQTTClient, MQTTSubscriber, MQTTMessage, MQTTConfig
    from iot_mqtt.config import get_feature_flags
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    logger.warning("[MQTT_RECEIVER] iot_mqtt module not available")


class MQTTIngestReceiver:
    """Receptor MQTT para ingesta.
    
    Escucha el topic iot/sensors/+/readings y procesa las lecturas
    a través del pipeline existente de ingesta.
    
    Uso:
        receiver = MQTTIngestReceiver(bridge)
        await receiver.start()
    """
    
    def __init__(
        self,
        bridge: "MQTTRedisBridge",
        config: Optional["MQTTConfig"] = None,
    ):
        self._bridge = bridge
        self._config = config
        self._client: Optional["MQTTClient"] = None
        self._subscriber: Optional["MQTTSubscriber"] = None
        self._running = False
        self._messages_received = 0
        self._messages_processed = 0
        self._messages_failed = 0
    
    async def start(self) -> bool:
        """Inicia el receptor MQTT.
        
        Returns:
            True si se inició correctamente
        """
        if not MQTT_AVAILABLE:
            logger.error("[MQTT_RECEIVER] Cannot start - iot_mqtt not available")
            return False
        
        flags = get_feature_flags()
        if not flags.mqtt_ingest_enabled:
            logger.info("[MQTT_RECEIVER] MQTT ingest disabled by feature flag")
            return False
        
        try:
            self._client = MQTTClient(
                config=self._config,
                client_id="service-ingest",
            )
            
            connected = await self._client.connect()
            if not connected:
                logger.error("[MQTT_RECEIVER] Failed to connect to MQTT broker")
                return False
            
            self._subscriber = MQTTSubscriber(self._client, self._config)
            
            @self._subscriber.on_reading
            async def handle_reading(topic: str, message: MQTTMessage) -> None:
                await self._process_reading(topic, message)
            
            success = await self._subscriber.subscribe_readings()
            if not success:
                logger.error("[MQTT_RECEIVER] Failed to subscribe to readings")
                return False
            
            self._running = True
            logger.info("[MQTT_RECEIVER] Started successfully")
            return True
            
        except Exception as e:
            logger.exception("[MQTT_RECEIVER] Start failed: %s", e)
            return False
    
    async def stop(self) -> None:
        """Detiene el receptor MQTT."""
        self._running = False
        
        if self._subscriber:
            await self._subscriber.unsubscribe_all()
        
        if self._client:
            await self._client.disconnect()
        
        logger.info(
            "[MQTT_RECEIVER] Stopped. Stats: received=%d processed=%d failed=%d",
            self._messages_received,
            self._messages_processed,
            self._messages_failed,
        )
    
    async def _process_reading(
        self,
        topic: str,
        message: MQTTMessage,
    ) -> None:
        """Procesa una lectura recibida por MQTT."""
        self._messages_received += 1
        start_time = time.perf_counter()
        
        try:
            data = message.to_dict()
            
            validation = validate_mqtt_reading(data)
            
            if not validation.valid:
                logger.warning(
                    "[MQTT_RECEIVER] Invalid message: %s (topic=%s)",
                    validation.error,
                    topic,
                )
                self._messages_failed += 1
                return
            
            if validation.warnings:
                for warn in validation.warnings:
                    logger.debug("[MQTT_RECEIVER] Warning: %s", warn)
            
            await self._bridge.process_reading(validation.payload)
            
            self._messages_processed += 1
            
            latency_ms = (time.perf_counter() - start_time) * 1000
            
            if self._messages_processed % 100 == 0:
                logger.info(
                    "[MQTT_RECEIVER] Processed %d messages (last latency: %.2fms)",
                    self._messages_processed,
                    latency_ms,
                )
                
        except Exception as e:
            logger.exception("[MQTT_RECEIVER] Processing error: %s", e)
            self._messages_failed += 1
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def stats(self) -> dict:
        return {
            "running": self._running,
            "messages_received": self._messages_received,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
        }

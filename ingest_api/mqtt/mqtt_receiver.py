"""Receptor MQTT para ingesta de lecturas.

Escucha mensajes MQTT y los procesa a través del pipeline de ingesta.
Compatibilidad 1:1 con HTTP endpoint /ingest/packets.

Flujo:
  MQTT topic iot/sensors/{id}/readings
  → mqtt_receiver (este archivo)
  → mqtt_bridge
  → Redis Stream readings:raw
  → flujo actual SIN CAMBIOS
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional, Callable, Any

from .validators import validate_mqtt_reading, MQTTReadingPayload

logger = logging.getLogger(__name__)

# Métricas Prometheus (opcional)
try:
    from prometheus_client import Counter, Histogram, Gauge
    MQTT_MESSAGES_RECEIVED = Counter(
        'mqtt_ingest_messages_received_total',
        'Total MQTT messages received',
        ['status']  # success, validation_error, processing_error
    )
    MQTT_PROCESSING_LATENCY = Histogram(
        'mqtt_ingest_processing_seconds',
        'MQTT message processing latency',
        buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
    )
    MQTT_RECEIVER_CONNECTED = Gauge(
        'mqtt_ingest_receiver_connected',
        'MQTT receiver connection status'
    )
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False

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
    
    COMPATIBILIDAD:
    - Mismo formato de mensaje que HTTP /ingest/packets
    - Misma deduplicación
    - Mismas validaciones
    - Mismo destino: Redis Stream readings:raw
    
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
        self._last_message_at: float = 0
        self._reconnect_count = 0
    
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
            logger.info("[MQTT_RECEIVER] MQTT ingest disabled by feature flag FF_MQTT_INGEST_ENABLED")
            return False
        
        try:
            # Configuración desde env o default
            if self._config is None:
                from iot_mqtt.config import get_mqtt_config
                self._config = get_mqtt_config()
            
            self._client = MQTTClient(
                config=self._config,
                client_id="service-ingest",
            )
            
            connected = await self._client.connect()
            if not connected:
                logger.error("[MQTT_RECEIVER] Failed to connect to MQTT broker at %s:%d",
                    self._config.broker_host, self._config.effective_port)
                return False
            
            self._subscriber = MQTTSubscriber(self._client, self._config)
            
            # Registrar handler para lecturas
            await self._subscriber.subscribe(
                topic="iot/sensors/+/readings",
                handler=self._process_reading,
            )
            
            self._running = True
            
            if METRICS_AVAILABLE:
                MQTT_RECEIVER_CONNECTED.set(1)
            
            logger.info(
                "[MQTT_RECEIVER] Started successfully - listening on iot/sensors/+/readings"
            )
            return True
            
        except Exception as e:
            logger.exception("[MQTT_RECEIVER] Start failed: %s", e)
            if METRICS_AVAILABLE:
                MQTT_RECEIVER_CONNECTED.set(0)
            return False
    
    async def stop(self) -> None:
        """Detiene el receptor MQTT."""
        self._running = False
        
        if METRICS_AVAILABLE:
            MQTT_RECEIVER_CONNECTED.set(0)
        
        if self._subscriber:
            try:
                await self._subscriber.unsubscribe_all()
            except Exception as e:
                logger.warning("[MQTT_RECEIVER] Error unsubscribing: %s", e)
        
        if self._client:
            try:
                await self._client.disconnect()
            except Exception as e:
                logger.warning("[MQTT_RECEIVER] Error disconnecting: %s", e)
        
        logger.info(
            "[MQTT_RECEIVER] Stopped. Stats: received=%d processed=%d failed=%d reconnects=%d",
            self._messages_received,
            self._messages_processed,
            self._messages_failed,
            self._reconnect_count,
        )
    
    async def _process_reading(
        self,
        topic: str,
        message: "MQTTMessage",
    ) -> None:
        """Procesa una lectura recibida por MQTT.
        
        FLUJO IDÉNTICO A HTTP:
        1. Validar payload
        2. Deduplicar
        3. Publicar a Redis Stream readings:raw
        """
        self._messages_received += 1
        self._last_message_at = time.time()
        start_time = time.perf_counter()
        
        try:
            # Extraer datos del mensaje MQTT
            data = message.to_dict()
            
            # Validación idéntica a HTTP
            validation = validate_mqtt_reading(data)
            
            if not validation.valid:
                logger.warning(
                    "[MQTT_RECEIVER] Invalid message: %s (topic=%s)",
                    validation.error,
                    topic,
                )
                self._messages_failed += 1
                if METRICS_AVAILABLE:
                    MQTT_MESSAGES_RECEIVED.labels(status='validation_error').inc()
                return
            
            if validation.warnings:
                for warn in validation.warnings:
                    logger.debug("[MQTT_RECEIVER] Warning: %s", warn)
            
            # Procesar a través del bridge (dedup + Redis)
            success = await self._bridge.process_reading(validation.payload)
            
            latency_ms = (time.perf_counter() - start_time) * 1000
            
            if success:
                self._messages_processed += 1
                if METRICS_AVAILABLE:
                    MQTT_MESSAGES_RECEIVED.labels(status='success').inc()
                    MQTT_PROCESSING_LATENCY.observe(latency_ms / 1000)
            else:
                self._messages_failed += 1
                if METRICS_AVAILABLE:
                    MQTT_MESSAGES_RECEIVED.labels(status='processing_error').inc()
            
            # Log periódico estructurado
            if self._messages_processed % 100 == 0:
                logger.info(
                    "[MQTT_RECEIVER] stats received=%d processed=%d failed=%d latency_ms=%.2f",
                    self._messages_received,
                    self._messages_processed,
                    self._messages_failed,
                    latency_ms,
                )
                
        except Exception as e:
            logger.exception("[MQTT_RECEIVER] Processing error: %s", e)
            self._messages_failed += 1
            if METRICS_AVAILABLE:
                MQTT_MESSAGES_RECEIVED.labels(status='processing_error').inc()
    
    @property
    def is_running(self) -> bool:
        return self._running
    
    @property
    def is_connected(self) -> bool:
        return self._client is not None and self._client.is_connected
    
    @property
    def stats(self) -> dict:
        return {
            "running": self._running,
            "connected": self.is_connected,
            "messages_received": self._messages_received,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
            "reconnect_count": self._reconnect_count,
            "last_message_at": self._last_message_at,
        }
    
    def health_check(self) -> dict:
        """Health check para monitoreo."""
        return {
            "healthy": self._running and self.is_connected,
            "running": self._running,
            "connected": self.is_connected,
            "messages_processed": self._messages_processed,
            "messages_failed": self._messages_failed,
            "last_message_age_seconds": time.time() - self._last_message_at if self._last_message_at > 0 else None,
        }

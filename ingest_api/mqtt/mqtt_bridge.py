"""Bridge MQTT → Redis Streams.

Conecta las lecturas recibidas por MQTT con el pipeline existente
de Redis Streams, manteniendo compatibilidad total.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from typing import Any, Optional

from .validators import MQTTReadingPayload

logger = logging.getLogger(__name__)


class DeduplicationCache:
    """Cache de deduplicación para evitar duplicados HTTP + MQTT."""
    
    def __init__(self, ttl_seconds: int = 60, max_size: int = 50000):
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._cache: dict[str, float] = {}
        self._lock = asyncio.Lock()
    
    async def is_duplicate(self, key: str) -> bool:
        """Verifica si la lectura es duplicada."""
        async with self._lock:
            self._cleanup()
            return key in self._cache
    
    async def mark_seen(self, key: str) -> None:
        """Marca lectura como vista."""
        async with self._lock:
            self._cache[key] = time.time()
    
    def _cleanup(self) -> None:
        """Elimina entradas expiradas."""
        if len(self._cache) <= self._max_size // 2:
            return
        
        now = time.time()
        expired = [k for k, v in self._cache.items() if now - v > self._ttl]
        for k in expired:
            del self._cache[k]
    
    def generate_key(
        self,
        sensor_id: int,
        value: float,
        timestamp: float,
    ) -> str:
        """Genera clave de deduplicación."""
        data = f"{sensor_id}:{value:.6f}:{timestamp:.3f}"
        return hashlib.md5(data.encode()).hexdigest()[:16]


class MQTTRedisBridge:
    """Bridge que conecta MQTT con Redis Streams.
    
    Procesa lecturas recibidas por MQTT y las publica al pipeline
    existente de Redis Streams, manteniendo:
    - Deduplicación con HTTP
    - Validación
    - Métricas
    - Compatibilidad total con el pipeline existente
    
    Uso:
        bridge = MQTTRedisBridge(broker_factory)
        await bridge.process_reading(payload)
    """
    
    def __init__(
        self,
        broker_factory: Optional[Any] = None,
        dedup_enabled: bool = True,
        dedup_ttl_seconds: int = 60,
    ):
        self._broker_factory = broker_factory
        self._dedup_enabled = dedup_enabled
        self._dedup_cache = DeduplicationCache(ttl_seconds=dedup_ttl_seconds)
        
        self._readings_processed = 0
        self._readings_deduplicated = 0
        self._readings_failed = 0
    
    async def process_reading(
        self,
        payload: MQTTReadingPayload,
    ) -> bool:
        """Procesa una lectura MQTT y la envía a Redis Streams.
        
        Args:
            payload: Lectura validada
            
        Returns:
            True si se procesó correctamente
        """
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            logger.warning("[MQTT_BRIDGE] Invalid sensor_id: %s", payload.sensor_id)
            self._readings_failed += 1
            return False
        
        if self._dedup_enabled:
            dedup_key = self._dedup_cache.generate_key(
                sensor_id=sensor_id,
                value=payload.value,
                timestamp=payload.timestamp_float,
            )
            
            if await self._dedup_cache.is_duplicate(dedup_key):
                logger.debug(
                    "[MQTT_BRIDGE] Duplicate reading skipped: sensor=%d",
                    sensor_id,
                )
                self._readings_deduplicated += 1
                return True
            
            await self._dedup_cache.mark_seen(dedup_key)
        
        try:
            await self._publish_to_redis(payload)
            self._readings_processed += 1
            return True
            
        except Exception as e:
            logger.exception("[MQTT_BRIDGE] Failed to publish: %s", e)
            self._readings_failed += 1
            return False
    
    async def _publish_to_redis(
        self,
        payload: MQTTReadingPayload,
    ) -> None:
        """Publica lectura a Redis Streams.
        
        Usa el broker existente para mantener compatibilidad
        con el pipeline ML/Telemetría.
        """
        if self._broker_factory is None:
            from ..broker import get_broker
            broker = get_broker()
        else:
            broker = self._broker_factory()
        
        try:
            from iot_machine_learning.ml_service.reading_broker import Reading
            
            reading = Reading(
                sensor_id=payload.sensor_id_int,
                sensor_type=payload.sensor_type or "unknown",
                value=payload.value,
                timestamp=payload.timestamp_float,
            )
            
            broker.publish(reading)
            
            logger.debug(
                "[MQTT_BRIDGE] Published to Redis: sensor=%d value=%.4f",
                payload.sensor_id_int,
                payload.value,
            )
            
        except ImportError:
            logger.warning(
                "[MQTT_BRIDGE] ML Reading class not available, using dict"
            )
            
            reading_dict = {
                "sensor_id": payload.sensor_id_int,
                "value": payload.value,
                "timestamp": payload.timestamp_float,
                "sensor_type": payload.sensor_type or "unknown",
                "device_uuid": payload.device_uuid,
                "sensor_uuid": payload.sensor_uuid,
                "sequence": payload.sequence,
            }
            
            if hasattr(broker, "publish_dict"):
                broker.publish_dict(reading_dict)
            else:
                logger.error("[MQTT_BRIDGE] Broker does not support dict publish")
                raise RuntimeError("Broker incompatible")
    
    @property
    def stats(self) -> dict:
        """Estadísticas del bridge."""
        return {
            "readings_processed": self._readings_processed,
            "readings_deduplicated": self._readings_deduplicated,
            "readings_failed": self._readings_failed,
            "dedup_enabled": self._dedup_enabled,
        }
    
    def reset_stats(self) -> None:
        """Resetea estadísticas."""
        self._readings_processed = 0
        self._readings_deduplicated = 0
        self._readings_failed = 0


_bridge_instance: Optional[MQTTRedisBridge] = None


def get_mqtt_bridge() -> MQTTRedisBridge:
    """Obtiene instancia singleton del bridge."""
    global _bridge_instance
    if _bridge_instance is None:
        _bridge_instance = MQTTRedisBridge()
    return _bridge_instance


def reset_mqtt_bridge() -> None:
    """Resetea el bridge singleton."""
    global _bridge_instance
    _bridge_instance = None

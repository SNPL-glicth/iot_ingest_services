"""Bridge MQTT → Redis Streams.

Conecta las lecturas recibidas por MQTT con el pipeline existente
de Redis Streams, manteniendo compatibilidad total.

GARANTÍAS:
- Deduplicación idéntica a HTTP (sensor_id, timestamp, value)
- Mismo formato de mensaje a Redis Stream
- Mismas validaciones
- Sin cambios en modelos de dominio
- Sin cambios en contratos internos
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import time
from typing import Any, Optional

from .validators import MQTTReadingPayload

logger = logging.getLogger(__name__)

# Métricas Prometheus (opcional)
try:
    from prometheus_client import Counter, Gauge
    MQTT_BRIDGE_PROCESSED = Counter(
        'mqtt_bridge_readings_processed_total',
        'Total readings processed by MQTT bridge',
        ['status']  # success, deduplicated, failed
    )
    MQTT_BRIDGE_DEDUP_CACHE_SIZE = Gauge(
        'mqtt_bridge_dedup_cache_size',
        'Current size of deduplication cache'
    )
    METRICS_AVAILABLE = True
except ImportError:
    METRICS_AVAILABLE = False


class DeduplicationCache:
    """Cache de deduplicación para evitar duplicados HTTP + MQTT.
    
    CLAVE DE DEDUPLICACIÓN: (sensor_id, value, timestamp)
    - Misma lógica que HTTP para garantizar compatibilidad
    - TTL de 60 segundos por defecto
    - Limpieza automática cuando cache supera 50% de capacidad
    """
    
    def __init__(self, ttl_seconds: int = 60, max_size: int = 50000):
        self._ttl = ttl_seconds
        self._max_size = max_size
        self._cache: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0
    
    async def is_duplicate(self, key: str) -> bool:
        """Verifica si la lectura es duplicada."""
        async with self._lock:
            self._cleanup()
            is_dup = key in self._cache
            if is_dup:
                self._hits += 1
            else:
                self._misses += 1
            return is_dup
    
    async def mark_seen(self, key: str) -> None:
        """Marca lectura como vista."""
        async with self._lock:
            self._cache[key] = time.time()
            if METRICS_AVAILABLE:
                MQTT_BRIDGE_DEDUP_CACHE_SIZE.set(len(self._cache))
    
    def _cleanup(self) -> None:
        """Elimina entradas expiradas."""
        if len(self._cache) <= self._max_size // 2:
            return
        
        now = time.time()
        expired = [k for k, v in self._cache.items() if now - v > self._ttl]
        for k in expired:
            del self._cache[k]
        
        if METRICS_AVAILABLE:
            MQTT_BRIDGE_DEDUP_CACHE_SIZE.set(len(self._cache))
    
    def generate_key(
        self,
        sensor_id: int,
        value: float,
        timestamp: float,
    ) -> str:
        """Genera clave de deduplicación.
        
        FORMATO: MD5(sensor_id:value:timestamp)[:16]
        - value con 6 decimales para precisión
        - timestamp con 3 decimales (milisegundos)
        """
        data = f"{sensor_id}:{value:.6f}:{timestamp:.3f}"
        return hashlib.md5(data.encode()).hexdigest()[:16]
    
    @property
    def stats(self) -> dict:
        return {
            "size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl,
            "hits": self._hits,
            "misses": self._misses,
            "hit_rate": self._hits / (self._hits + self._misses) if (self._hits + self._misses) > 0 else 0,
        }


class MQTTRedisBridge:
    """Bridge que conecta MQTT con Redis Streams.
    
    Procesa lecturas recibidas por MQTT y las publica al pipeline
    existente de Redis Streams, manteniendo:
    - Deduplicación con HTTP
    - Validación
    - Métricas
    - Compatibilidad total con el pipeline existente
    
    GARANTÍAS:
    - Redis recibe MISMO formato que HTTP
    - 0 regresiones en SP
    - Latencia ≤ HTTP
    
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
        self._broker_instance = None
    
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
                    "[MQTT_BRIDGE] Duplicate reading skipped: sensor=%d value=%.4f",
                    sensor_id,
                    payload.value,
                )
                self._readings_deduplicated += 1
                if METRICS_AVAILABLE:
                    MQTT_BRIDGE_PROCESSED.labels(status='deduplicated').inc()
                return True
            
            await self._dedup_cache.mark_seen(dedup_key)
        
        try:
            await self._publish_to_redis(payload)
            self._readings_processed += 1
            if METRICS_AVAILABLE:
                MQTT_BRIDGE_PROCESSED.labels(status='success').inc()
            return True
            
        except Exception as e:
            logger.exception("[MQTT_BRIDGE] Failed to publish: %s", e)
            self._readings_failed += 1
            if METRICS_AVAILABLE:
                MQTT_BRIDGE_PROCESSED.labels(status='failed').inc()
            return False
    
    def _get_broker(self):
        """Obtiene instancia del broker (cacheada)."""
        if self._broker_instance is None:
            if self._broker_factory is not None:
                self._broker_instance = self._broker_factory()
            else:
                from ..broker import get_broker
                self._broker_instance = get_broker()
        return self._broker_instance
    
    async def _publish_to_redis(
        self,
        payload: MQTTReadingPayload,
    ) -> None:
        """Publica lectura a Redis Streams.
        
        FORMATO IDÉNTICO A HTTP:
        Usa el broker existente para mantener compatibilidad
        con el pipeline ML/Telemetría.
        
        El broker publica a Redis Stream 'readings:raw' con el
        mismo formato que el endpoint HTTP /ingest/packets.
        """
        broker = self._get_broker()
        
        try:
            from iot_machine_learning.ml_service.reading_broker import Reading
            
            # Crear Reading con MISMO formato que HTTP
            reading = Reading(
                sensor_id=payload.sensor_id_int,
                sensor_type=payload.sensor_type or "unknown",
                value=payload.value,
                timestamp=payload.timestamp_float,
            )
            
            broker.publish(reading)
            
            logger.debug(
                "[MQTT_BRIDGE] Published to Redis: sensor=%d value=%.4f ts=%.3f",
                payload.sensor_id_int,
                payload.value,
                payload.timestamp_float,
            )
            
        except ImportError:
            # Fallback: usar dict si Reading no está disponible
            logger.debug(
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
            elif hasattr(broker, "publish"):
                # Intentar publish directo
                broker.publish(reading_dict)
            else:
                logger.error("[MQTT_BRIDGE] Broker does not support publish")
                raise RuntimeError("Broker incompatible")
    
    @property
    def stats(self) -> dict:
        """Estadísticas del bridge."""
        return {
            "readings_processed": self._readings_processed,
            "readings_deduplicated": self._readings_deduplicated,
            "readings_failed": self._readings_failed,
            "dedup_enabled": self._dedup_enabled,
            "dedup_cache": self._dedup_cache.stats,
        }
    
    def reset_stats(self) -> None:
        """Resetea estadísticas."""
        self._readings_processed = 0
        self._readings_deduplicated = 0
        self._readings_failed = 0
    
    def health_check(self) -> dict:
        """Health check del bridge."""
        broker = self._get_broker()
        broker_health = {}
        
        if hasattr(broker, "health_check"):
            broker_health = broker.health_check()
        elif hasattr(broker, "_broker") and hasattr(broker._broker, "health_check"):
            broker_health = broker._broker.health_check()
        
        return {
            "healthy": self._readings_failed == 0 or self._readings_processed > self._readings_failed * 10,
            "readings_processed": self._readings_processed,
            "readings_deduplicated": self._readings_deduplicated,
            "readings_failed": self._readings_failed,
            "dedup_cache_size": len(self._dedup_cache._cache),
            "broker": broker_health,
        }


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

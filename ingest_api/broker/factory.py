"""Factory para crear instancias del broker.

Centraliza la configuración y creación del broker.

REFACTORIZADO 2026-01-29:
- Integración REAL de Redis Streams
- Fallback automático a InMemory si Redis no está disponible
- Throttling configurable
"""

from __future__ import annotations

import os
import logging
from typing import Optional

from iot_machine_learning.ml_service.reading_broker import ReadingBroker
from iot_machine_learning.ml_service.in_memory_broker import InMemoryReadingBroker
from iot_machine_learning.ml_service.broker import (
    RedisReadingBroker,
    BrokerType,
)

from .throttled import ThrottledReadingBroker

logger = logging.getLogger(__name__)

_broker_instance: Optional[ReadingBroker] = None


def create_broker(
    min_interval_seconds: Optional[float] = None,
    use_redis: Optional[bool] = None,
    redis_url: Optional[str] = None,
) -> ReadingBroker:
    """Crea una nueva instancia del broker.
    
    Args:
        min_interval_seconds: Intervalo mínimo entre publicaciones por sensor.
                              Default: ML_PUBLISH_MIN_INTERVAL_SECONDS env var o 1.0
        use_redis: Si True, usa Redis Streams. Si False, usa InMemory.
                   Default: USE_REDIS_BROKER env var o True
        redis_url: URL de conexión a Redis.
                   Default: REDIS_URL env var o redis://localhost:6379/0
    
    Returns:
        ReadingBroker configurado con throttling
        
    Environment Variables:
        ML_PUBLISH_MIN_INTERVAL_SECONDS: Intervalo mínimo entre publicaciones
        USE_REDIS_BROKER: "true" o "false" para habilitar/deshabilitar Redis
        REDIS_URL: URL de conexión a Redis
    """
    if min_interval_seconds is None:
        min_interval_seconds = float(
            os.getenv("ML_PUBLISH_MIN_INTERVAL_SECONDS", "1.0")
        )
    
    if use_redis is None:
        use_redis = os.getenv("USE_REDIS_BROKER", "true").lower() == "true"
    
    # Seleccionar broker base
    base_broker: ReadingBroker
    
    if use_redis:
        try:
            redis_broker = RedisReadingBroker(redis_url=redis_url)
            health = redis_broker.health_check()
            
            if health.get("connected"):
                logger.info(
                    "[BROKER_FACTORY] Using Redis Streams broker: %s",
                    health.get("redis_url"),
                )
                base_broker = redis_broker
            else:
                raise ConnectionError(health.get("last_error", "Connection failed"))
                
        except Exception as e:
            logger.warning(
                "[BROKER_FACTORY] Redis unavailable (%s), falling back to InMemory",
                str(e),
            )
            base_broker = InMemoryReadingBroker()
    else:
        logger.info("[BROKER_FACTORY] Using InMemory broker (Redis disabled)")
        base_broker = InMemoryReadingBroker()
    
    # Aplicar throttling
    return ThrottledReadingBroker(
        base_broker,
        min_interval_seconds=min_interval_seconds,
    )


def get_broker() -> ReadingBroker:
    """Obtiene la instancia singleton del broker.
    
    Crea el broker en la primera llamada y lo reutiliza después.
    """
    global _broker_instance
    if _broker_instance is None:
        _broker_instance = create_broker()
    return _broker_instance


def reset_broker() -> None:
    """Resetea el broker singleton (útil para testing)."""
    global _broker_instance
    if _broker_instance is not None:
        if hasattr(_broker_instance, "stop"):
            # ThrottledReadingBroker wraps the real broker
            inner = getattr(_broker_instance, "_broker", None)
            if inner and hasattr(inner, "stop"):
                inner.stop()
    _broker_instance = None


def get_broker_health() -> dict:
    """Obtiene el estado de salud del broker actual."""
    broker = get_broker()
    
    # ThrottledReadingBroker wraps the real broker
    inner = getattr(broker, "_broker", broker)
    
    if hasattr(inner, "health_check"):
        health = inner.health_check()
        health["throttled"] = True
        health["throttle_interval_seconds"] = getattr(broker, "_min_interval", None)
        return health
    
    return {
        "type": type(inner).__name__,
        "connected": True,
        "throttled": True,
        "note": "InMemory broker - no persistence",
    }

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

from ..core.domain.broker_interface import IReadingBroker, NullBroker

# Import condicional de ML
try:
    from iot_machine_learning.ml_service.reading_broker import ReadingBroker
    from iot_machine_learning.ml_service.in_memory_broker import InMemoryReadingBroker
    from iot_machine_learning.ml_service.broker import RedisReadingBroker, BrokerType
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    ReadingBroker = None
    InMemoryReadingBroker = None
    RedisReadingBroker = None
    BrokerType = None

from .throttled import ThrottledReadingBroker

logger = logging.getLogger(__name__)

_broker_instance: Optional[IReadingBroker] = None


def create_broker(
    min_interval_seconds: Optional[float] = None,
    use_redis: Optional[bool] = None,
    redis_url: Optional[str] = None,
) -> IReadingBroker:
    """Crea una nueva instancia del broker.
    
    Returns NullBroker if ML module is not available.
    """
    if not ML_AVAILABLE:
        logger.warning("[BROKER_FACTORY] ML not available, using NullBroker")
        return NullBroker()
    
    if min_interval_seconds is None:
        min_interval_seconds = float(
            os.getenv("ML_PUBLISH_MIN_INTERVAL_SECONDS", "1.0")
        )
    
    if use_redis is None:
        use_redis = os.getenv("USE_REDIS_BROKER", "true").lower() == "true"
    
    # Seleccionar broker base
    base_broker: IReadingBroker
    
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


def get_broker() -> IReadingBroker:
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

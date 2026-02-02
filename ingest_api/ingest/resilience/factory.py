"""Factory para inicializar componentes de resiliencia.
para iniciar mi socio con el de resiencia
Proporciona funciones para crear instancias de deduplicador y DLQ
con configuración desde variables de entorno.
"""

from __future__ import annotations

import logging
import os
from typing import Optional, Tuple

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from .deduplication import MessageDeduplicator
from .dead_letter import DeadLetterQueue

logger = logging.getLogger(__name__)

# Singleton instances
_redis_client: Optional["redis.Redis"] = None
_deduplicator: Optional[MessageDeduplicator] = None
_dlq: Optional[DeadLetterQueue] = None


def get_redis_client() -> Optional["redis.Redis"]:
    """Obtiene o crea el cliente Redis singleton.
    
    Configuración via variables de entorno:
    - REDIS_URL: URL de conexión (default: redis://localhost:6379/0)
    - REDIS_ENABLED: "true" para habilitar (default: "true")
    
    Returns:
        Cliente Redis o None si no está disponible/habilitado.
    """
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    if not REDIS_AVAILABLE:
        logger.warning("RESILIENCE redis package not installed")
        return None
    
    enabled = os.getenv("REDIS_ENABLED", "true").lower() == "true"
    if not enabled:
        logger.info("RESILIENCE Redis disabled by REDIS_ENABLED=false")
        return None
    
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    try:
        _redis_client = redis.from_url(
            redis_url,
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        # Test connection
        _redis_client.ping()
        logger.info("RESILIENCE Redis connected: %s", redis_url.split("@")[-1])
        return _redis_client
        
    except Exception as e:
        logger.warning("RESILIENCE Redis connection failed: %s", e)
        _redis_client = None
        return None


def get_deduplicator() -> Optional[MessageDeduplicator]:
    """Obtiene o crea el deduplicador singleton.
    
    Configuración via variables de entorno:
    - DEDUP_ENABLED: "true" para habilitar (default: "true")
    - DEDUP_TTL_SECONDS: TTL de registros (default: 300)
    
    Returns:
        MessageDeduplicator o None si no está disponible.
    """
    global _deduplicator
    
    if _deduplicator is not None:
        return _deduplicator
    
    enabled = os.getenv("DEDUP_ENABLED", "true").lower() == "true"
    if not enabled:
        logger.info("RESILIENCE Deduplication disabled by DEDUP_ENABLED=false")
        return None
    
    redis_client = get_redis_client()
    if redis_client is None:
        logger.warning("RESILIENCE Deduplication disabled (no Redis)")
        return None
    
    ttl = int(os.getenv("DEDUP_TTL_SECONDS", "300"))
    
    _deduplicator = MessageDeduplicator(
        redis_client=redis_client,
        ttl_seconds=ttl,
    )
    logger.info("RESILIENCE Deduplicator initialized (ttl=%ds)", ttl)
    return _deduplicator


def get_dlq() -> Optional[DeadLetterQueue]:
    """Obtiene o crea la DLQ singleton.
    
    Configuración via variables de entorno:
    - DLQ_ENABLED: "true" para habilitar (default: "true")
    - DLQ_STREAM_NAME: Nombre del stream (default: "dlq:ingest")
    - DLQ_MAX_LEN: Máximo de entradas (default: 10000)
    
    Returns:
        DeadLetterQueue o None si no está disponible.
    """
    global _dlq
    
    if _dlq is not None:
        return _dlq
    
    enabled = os.getenv("DLQ_ENABLED", "true").lower() == "true"
    if not enabled:
        logger.info("RESILIENCE DLQ disabled by DLQ_ENABLED=false")
        return None
    
    redis_client = get_redis_client()
    if redis_client is None:
        logger.warning("RESILIENCE DLQ disabled (no Redis)")
        return None
    
    stream_name = os.getenv("DLQ_STREAM_NAME", "dlq:ingest")
    max_len = int(os.getenv("DLQ_MAX_LEN", "10000"))
    
    _dlq = DeadLetterQueue(
        redis_client=redis_client,
        stream_name=stream_name,
        max_len=max_len,
    )
    logger.info("RESILIENCE DLQ initialized (stream=%s, max=%d)", stream_name, max_len)
    return _dlq


def get_resilience_components() -> Tuple[Optional[MessageDeduplicator], Optional[DeadLetterQueue]]:
    """Obtiene ambos componentes de resiliencia.
    
    Returns:
        Tupla (deduplicator, dlq), cualquiera puede ser None.
    """
    return get_deduplicator(), get_dlq()


def reset_resilience():
    """Resetea los singletons (útil para tests)."""
    global _redis_client, _deduplicator, _dlq
    
    if _redis_client is not None:
        try:
            _redis_client.close()
        except Exception:
            pass
    
    _redis_client = None
    _deduplicator = None
    _dlq = None
    logger.info("RESILIENCE components reset")


def get_resilience_health() -> dict:
    """Obtiene el estado de salud de los componentes de resiliencia.
    
    Returns:
        Diccionario con estado de cada componente.
    """
    health = {
        "redis_available": REDIS_AVAILABLE,
        "redis_connected": False,
        "deduplicator": None,
        "dlq": None,
    }
    
    if _redis_client is not None:
        try:
            _redis_client.ping()
            health["redis_connected"] = True
        except Exception:
            health["redis_connected"] = False
    
    if _deduplicator is not None:
        health["deduplicator"] = _deduplicator.stats
    
    if _dlq is not None:
        health["dlq"] = _dlq.stats
    
    return health

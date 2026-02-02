"""Deduplicación de mensajes con Redis.
para evitar locuras mi bro
Evita procesar mensajes duplicados usando un SET con TTL en Redis.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

logger = logging.getLogger(__name__)


class MessageDeduplicator:
    """Deduplicador de mensajes usando Redis SET con TTL.
    
    Cada mensaje se identifica por un msg_id único. Si el msg_id ya existe
    en Redis, el mensaje es considerado duplicado y debe descartarse.
    
    Attributes:
        ttl_seconds: Tiempo de vida del registro de deduplicación (default: 300s = 5min)
        key_prefix: Prefijo para las claves en Redis
    """
    
    DEFAULT_TTL = 300  # 5 minutos
    KEY_PREFIX = "dedup:msg:"
    
    def __init__(
        self,
        redis_client: Optional["redis.Redis"] = None,
        ttl_seconds: int = DEFAULT_TTL,
    ):
        """Inicializa el deduplicador.
        
        Args:
            redis_client: Cliente Redis. Si es None, deduplicación deshabilitada.
            ttl_seconds: TTL para registros de deduplicación.
        """
        self._redis = redis_client
        self._ttl = ttl_seconds
        self._enabled = redis_client is not None and REDIS_AVAILABLE
        
        # Stats
        self._total_checked = 0
        self._duplicates_found = 0
    
    @property
    def enabled(self) -> bool:
        """Indica si la deduplicación está habilitada."""
        return self._enabled
    
    @property
    def stats(self) -> dict:
        """Estadísticas de deduplicación."""
        return {
            "enabled": self._enabled,
            "total_checked": self._total_checked,
            "duplicates_found": self._duplicates_found,
            "ttl_seconds": self._ttl,
        }
    
    def is_duplicate(self, msg_id: str) -> bool:
        """Verifica si un mensaje es duplicado.
        
        Args:
            msg_id: Identificador único del mensaje.
            
        Returns:
            True si el mensaje ya fue procesado (duplicado), False si es nuevo.
        """
        if not self._enabled or not msg_id:
            return False
        
        self._total_checked += 1
        key = f"{self.KEY_PREFIX}{msg_id}"
        
        try:
            # SET NX = solo si no existe, retorna None si ya existía
            result = self._redis.set(key, "1", nx=True, ex=self._ttl)
            
            if result is None:
                # Ya existía = duplicado
                self._duplicates_found += 1
                logger.debug("DEDUP duplicate msg_id=%s", msg_id)
                return True
            
            return False
            
        except Exception as e:
            # Si Redis falla, permitir el mensaje (fail open)
            logger.warning("DEDUP redis_error msg_id=%s err=%s", msg_id, e)
            return False
    
    def generate_msg_id(
        self,
        sensor_id: int,
        timestamp: float,
        value: float,
    ) -> str:
        """Genera un msg_id basado en sensor_id + timestamp + value.
        
        Útil cuando el mensaje no trae msg_id explícito.
        
        Args:
            sensor_id: ID del sensor
            timestamp: Timestamp Unix de la lectura
            value: Valor de la lectura
            
        Returns:
            msg_id generado
        """
        # Usar precisión de 6 decimales para timestamp y value
        return f"{sensor_id}:{timestamp:.6f}:{value:.6f}"
    
    def clear_stats(self):
        """Limpia las estadísticas."""
        self._total_checked = 0
        self._duplicates_found = 0

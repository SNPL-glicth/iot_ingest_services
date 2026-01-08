"""Rate Limiter para API de Ingesta IoT.

SSOT: Este módulo es la fuente única de verdad para rate limiting en ingesta.

Estrategia:
- Por sensor: Máx N lecturas por minuto por sensor
- Por dispositivo: Máx M lecturas por minuto por dispositivo
- Global: Máx X requests por minuto por IP

Configuración via env vars:
- RATE_LIMIT_SENSOR_PER_MIN (default: 60)
- RATE_LIMIT_DEVICE_PER_MIN (default: 300)
- RATE_LIMIT_GLOBAL_PER_MIN (default: 1000)
- RATE_LIMIT_ENABLED (default: 1)
"""

from __future__ import annotations

import os
import time
import logging
from collections import defaultdict
from dataclasses import dataclass
from threading import Lock
from typing import Dict, Optional, Tuple

from fastapi import HTTPException, Request


logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuración de rate limiting."""
    sensor_per_min: int = 60      # Lecturas por sensor por minuto
    device_per_min: int = 300     # Lecturas por dispositivo por minuto
    global_per_min: int = 1000    # Requests por IP por minuto
    enabled: bool = True
    
    @classmethod
    def from_env(cls) -> "RateLimitConfig":
        return cls(
            sensor_per_min=int(os.getenv("RATE_LIMIT_SENSOR_PER_MIN", "60")),
            device_per_min=int(os.getenv("RATE_LIMIT_DEVICE_PER_MIN", "300")),
            global_per_min=int(os.getenv("RATE_LIMIT_GLOBAL_PER_MIN", "1000")),
            enabled=os.getenv("RATE_LIMIT_ENABLED", "1").strip() in ("1", "true", "yes"),
        )


class SlidingWindowCounter:
    """Contador de ventana deslizante para rate limiting.
    
    Usa un algoritmo de ventana deslizante aproximada para eficiencia:
    - Divide el tiempo en ventanas de 1 minuto
    - Mantiene conteos de la ventana actual y anterior
    - Calcula el rate aproximado interpolando entre ventanas
    """
    
    def __init__(self, window_seconds: int = 60):
        self._window_seconds = window_seconds
        self._lock = Lock()
        # key -> (prev_count, curr_count, curr_window_start)
        self._counters: Dict[str, Tuple[int, int, float]] = {}
    
    def _get_window_start(self, timestamp: float) -> float:
        """Obtiene el inicio de la ventana para un timestamp."""
        return (timestamp // self._window_seconds) * self._window_seconds
    
    def increment_and_check(self, key: str, limit: int) -> Tuple[bool, int]:
        """Incrementa el contador y verifica si excede el límite.
        
        Args:
            key: Identificador único (ej: sensor_id, device_uuid, ip)
            limit: Límite máximo por ventana
            
        Returns:
            Tuple (allowed: bool, current_count: int)
        """
        now = time.time()
        window_start = self._get_window_start(now)
        
        with self._lock:
            prev_count, curr_count, stored_window = self._counters.get(key, (0, 0, window_start))
            
            # Si estamos en una nueva ventana, rotar contadores
            if stored_window < window_start:
                # La ventana anterior se vuelve "prev", la actual empieza en 0
                if stored_window == window_start - self._window_seconds:
                    prev_count = curr_count
                else:
                    # Más de una ventana ha pasado, no hay datos previos relevantes
                    prev_count = 0
                curr_count = 0
                stored_window = window_start
            
            # Incrementar contador actual
            curr_count += 1
            self._counters[key] = (prev_count, curr_count, stored_window)
            
            # Calcular rate aproximado usando ventana deslizante
            # Peso de la ventana anterior basado en qué tan avanzados estamos en la actual
            elapsed_in_window = now - window_start
            prev_weight = 1 - (elapsed_in_window / self._window_seconds)
            approx_count = int(prev_count * prev_weight) + curr_count
            
            allowed = approx_count <= limit
            
            if not allowed:
                logger.warning(
                    "RATE_LIMIT_EXCEEDED key=%s approx_count=%d limit=%d",
                    key, approx_count, limit
                )
            
            return allowed, approx_count
    
    def cleanup_old_entries(self, max_age_seconds: int = 300) -> int:
        """Limpia entradas antiguas para evitar memory leak.
        
        Returns:
            Número de entradas eliminadas
        """
        now = time.time()
        cutoff = now - max_age_seconds
        
        with self._lock:
            old_keys = [
                k for k, (_, _, window_start) in self._counters.items()
                if window_start < cutoff
            ]
            for k in old_keys:
                del self._counters[k]
            
            return len(old_keys)


class IngestRateLimiter:
    """Rate limiter para la API de ingesta.
    
    Implementa tres niveles de rate limiting:
    1. Por sensor: Evita que un sensor individual sature el sistema
    2. Por dispositivo: Evita que un dispositivo comprometa el sistema
    3. Por IP: Evita ataques DoS desde una IP específica
    """
    
    def __init__(self, config: Optional[RateLimitConfig] = None):
        self.config = config or RateLimitConfig.from_env()
        self._sensor_counter = SlidingWindowCounter(window_seconds=60)
        self._device_counter = SlidingWindowCounter(window_seconds=60)
        self._ip_counter = SlidingWindowCounter(window_seconds=60)
        self._last_cleanup = time.time()
        self._cleanup_interval = 60  # Limpiar cada minuto
    
    def _maybe_cleanup(self) -> None:
        """Limpieza periódica de contadores antiguos."""
        now = time.time()
        if now - self._last_cleanup > self._cleanup_interval:
            cleaned = (
                self._sensor_counter.cleanup_old_entries() +
                self._device_counter.cleanup_old_entries() +
                self._ip_counter.cleanup_old_entries()
            )
            if cleaned > 0:
                logger.debug("RATE_LIMIT_CLEANUP removed=%d entries", cleaned)
            self._last_cleanup = now
    
    def check_sensor(self, sensor_id: int) -> Tuple[bool, int]:
        """Verifica rate limit para un sensor específico."""
        if not self.config.enabled:
            return True, 0
        
        self._maybe_cleanup()
        key = f"sensor:{sensor_id}"
        return self._sensor_counter.increment_and_check(key, self.config.sensor_per_min)
    
    def check_device(self, device_uuid: str) -> Tuple[bool, int]:
        """Verifica rate limit para un dispositivo específico."""
        if not self.config.enabled:
            return True, 0
        
        key = f"device:{device_uuid.lower()}"
        return self._device_counter.increment_and_check(key, self.config.device_per_min)
    
    def check_ip(self, ip: str) -> Tuple[bool, int]:
        """Verifica rate limit para una IP específica."""
        if not self.config.enabled:
            return True, 0
        
        key = f"ip:{ip}"
        return self._ip_counter.increment_and_check(key, self.config.global_per_min)
    
    def check_all(
        self,
        *,
        sensor_ids: Optional[list[int]] = None,
        device_uuid: Optional[str] = None,
        ip: Optional[str] = None,
    ) -> None:
        """Verifica todos los rate limits aplicables.
        
        Raises:
            HTTPException(429) si se excede cualquier límite
        """
        if not self.config.enabled:
            return
        
        # 1. Verificar IP primero (más amplio)
        if ip:
            allowed, count = self.check_ip(ip)
            if not allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded for IP. Try again in 60 seconds.",
                    headers={"Retry-After": "60", "X-RateLimit-Limit": str(self.config.global_per_min)},
                )
        
        # 2. Verificar dispositivo
        if device_uuid:
            allowed, count = self.check_device(device_uuid)
            if not allowed:
                raise HTTPException(
                    status_code=429,
                    detail=f"Rate limit exceeded for device. Max {self.config.device_per_min}/min.",
                    headers={"Retry-After": "60", "X-RateLimit-Limit": str(self.config.device_per_min)},
                )
        
        # 3. Verificar sensores individuales
        if sensor_ids:
            for sensor_id in sensor_ids:
                allowed, count = self.check_sensor(sensor_id)
                if not allowed:
                    raise HTTPException(
                        status_code=429,
                        detail=f"Rate limit exceeded for sensor {sensor_id}. Max {self.config.sensor_per_min}/min.",
                        headers={"Retry-After": "60", "X-RateLimit-Limit": str(self.config.sensor_per_min)},
                    )


# Singleton global para la aplicación
_rate_limiter: Optional[IngestRateLimiter] = None


def get_rate_limiter() -> IngestRateLimiter:
    """Obtiene el rate limiter singleton."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = IngestRateLimiter()
        logger.info(
            "RATE_LIMITER_INIT enabled=%s sensor=%d/min device=%d/min ip=%d/min",
            _rate_limiter.config.enabled,
            _rate_limiter.config.sensor_per_min,
            _rate_limiter.config.device_per_min,
            _rate_limiter.config.global_per_min,
        )
    return _rate_limiter


def get_client_ip(request: Request) -> str:
    """Obtiene la IP del cliente, considerando proxies."""
    # X-Forwarded-For puede tener múltiples IPs: "client, proxy1, proxy2"
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        # La primera IP es el cliente original
        return forwarded.split(",")[0].strip()
    
    # X-Real-IP es común en Nginx
    real_ip = request.headers.get("X-Real-IP")
    if real_ip:
        return real_ip.strip()
    
    # Fallback al cliente directo
    if request.client:
        return request.client.host
    
    return "unknown"

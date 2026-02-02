"""Backpressure para MQTT receiver.

FIX 2026-02-02: Implementa backpressure para evitar OOM en ráfagas (Sprint 3).
Refactorizado: Config y Stats extraídos a backpressure_config.py.
"""

from __future__ import annotations

import logging
import time
import threading
from collections import deque
from typing import Optional, TypeVar, Generic

from .backpressure_config import BackpressureConfig, BackpressureStats

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackpressureQueue(Generic[T]):
    """Cola con backpressure para mensajes MQTT.
    
    Características:
    - Límite de tamaño configurable
    - Drop oldest/newest cuando se llena
    - Rate limiting opcional
    - Thread-safe
    
    Uso:
        queue = BackpressureQueue[dict](max_size=1000)
        
        # Productor
        queue.put(message)
        
        # Consumidor
        message = queue.get(timeout=1.0)
    """
    
    def __init__(self, config: Optional[BackpressureConfig] = None):
        self._config = config or BackpressureConfig.from_env()
        self._queue: deque[T] = deque(maxlen=None)  # Manejamos límite manualmente
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        
        self._stats = BackpressureStats(max_size=self._config.max_queue_size)
        
        # Rate limiting
        self._last_enqueue_time: float = 0
        self._min_interval = (
            1.0 / self._config.rate_limit_per_sec 
            if self._config.rate_limit_per_sec > 0 
            else 0
        )
        
        logger.info(
            "BackpressureQueue initialized: max_size=%d, rate_limit=%.1f/s, drop_oldest=%s",
            self._config.max_queue_size,
            self._config.rate_limit_per_sec,
            self._config.drop_oldest,
        )
    
    def put(self, item: T) -> bool:
        """Agrega un item a la cola.
        
        Returns:
            True si se agregó, False si fue rate-limited
        """
        with self._lock:
            # Rate limiting
            if self._min_interval > 0:
                now = time.time()
                elapsed = now - self._last_enqueue_time
                if elapsed < self._min_interval:
                    self._stats.rate_limited += 1
                    return False
                self._last_enqueue_time = now
            
            # Backpressure: drop si está lleno
            if len(self._queue) >= self._config.max_queue_size:
                if self._config.drop_oldest:
                    self._queue.popleft()
                    self._stats.dropped += 1
                    logger.debug("Backpressure: dropped oldest message")
                else:
                    self._stats.dropped += 1
                    logger.debug("Backpressure: dropped newest message")
                    return True  # No agregamos el nuevo
            
            self._queue.append(item)
            self._stats.enqueued += 1
            self._stats.current_size = len(self._queue)
            
            self._not_empty.notify()
            return True
    
    def get(self, timeout: Optional[float] = None) -> Optional[T]:
        """Obtiene un item de la cola.
        
        Args:
            timeout: Segundos a esperar (None = bloquear indefinidamente)
            
        Returns:
            Item o None si timeout
        """
        with self._not_empty:
            if not self._queue:
                if timeout is None:
                    self._not_empty.wait()
                else:
                    self._not_empty.wait(timeout)
            
            if not self._queue:
                return None
            
            item = self._queue.popleft()
            self._stats.dequeued += 1
            self._stats.current_size = len(self._queue)
            return item
    
    def get_nowait(self) -> Optional[T]:
        """Obtiene un item sin esperar."""
        with self._lock:
            if not self._queue:
                return None
            
            item = self._queue.popleft()
            self._stats.dequeued += 1
            self._stats.current_size = len(self._queue)
            return item
    
    def get_batch(self, max_items: int, timeout: Optional[float] = None) -> list[T]:
        """Obtiene múltiples items de la cola.
        
        Args:
            max_items: Máximo de items a obtener
            timeout: Segundos a esperar por al menos 1 item
            
        Returns:
            Lista de items (puede estar vacía)
        """
        with self._not_empty:
            if not self._queue:
                if timeout is not None:
                    self._not_empty.wait(timeout)
            
            items = []
            while self._queue and len(items) < max_items:
                items.append(self._queue.popleft())
                self._stats.dequeued += 1
            
            self._stats.current_size = len(self._queue)
            return items
    
    def clear(self) -> int:
        """Limpia la cola.
        
        Returns:
            Número de items eliminados
        """
        with self._lock:
            count = len(self._queue)
            self._queue.clear()
            self._stats.current_size = 0
            return count
    
    @property
    def size(self) -> int:
        """Tamaño actual de la cola."""
        with self._lock:
            return len(self._queue)
    
    @property
    def is_full(self) -> bool:
        """Indica si la cola está llena."""
        with self._lock:
            return len(self._queue) >= self._config.max_queue_size
    
    @property
    def is_empty(self) -> bool:
        """Indica si la cola está vacía."""
        with self._lock:
            return len(self._queue) == 0
    
    def get_stats(self) -> dict:
        """Estadísticas de la cola."""
        with self._lock:
            return {
                "enqueued": self._stats.enqueued,
                "dequeued": self._stats.dequeued,
                "dropped": self._stats.dropped,
                "rate_limited": self._stats.rate_limited,
                "current_size": len(self._queue),
                "max_size": self._config.max_queue_size,
                "utilization_pct": (len(self._queue) / self._config.max_queue_size * 100)
                    if self._config.max_queue_size > 0 else 0,
            }


# Singleton para MQTT
_mqtt_queue: Optional[BackpressureQueue] = None


def get_mqtt_queue() -> BackpressureQueue:
    """Obtiene la cola singleton para MQTT."""
    global _mqtt_queue
    if _mqtt_queue is None:
        _mqtt_queue = BackpressureQueue()
    return _mqtt_queue


def reset_mqtt_queue() -> None:
    """Resetea la cola MQTT."""
    global _mqtt_queue
    if _mqtt_queue:
        _mqtt_queue.clear()
    _mqtt_queue = None

"""Circuit Breaker para proteger la base de datos.

FIX 2026-02-02: Implementa circuit breaker para evitar cascada de fallos (Sprint 3).
Refactorizado: Config y modelos extraídos a circuit_breaker_config.py.
"""

from __future__ import annotations

import logging
import time
import threading
from typing import Callable, TypeVar, Optional
from functools import wraps

from .circuit_breaker_config import CircuitState, CircuitBreakerConfig, CircuitBreakerOpen

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitBreaker:
    """Circuit breaker para proteger recursos externos.
    
    Uso:
        cb = CircuitBreaker("database")
        
        try:
            result = cb.call(lambda: db.execute(query))
        except CircuitBreakerOpen as e:
            # Circuito abierto, usar fallback
            return cached_result
    """
    
    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        self.name = name
        self._config = config or CircuitBreakerConfig.from_env()
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0
        self._lock = threading.Lock()
        
        logger.info(
            "CircuitBreaker '%s' initialized: failure_threshold=%d, "
            "recovery_timeout=%.1fs, success_threshold=%d",
            name,
            self._config.failure_threshold,
            self._config.recovery_timeout_seconds,
            self._config.success_threshold,
        )
    
    @property
    def state(self) -> CircuitState:
        """Estado actual del circuito."""
        with self._lock:
            return self._state
    
    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED
    
    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN
    
    def call(self, func: Callable[[], T]) -> T:
        """Ejecuta una función protegida por el circuit breaker.
        
        Args:
            func: Función a ejecutar
            
        Returns:
            Resultado de la función
            
        Raises:
            CircuitBreakerOpen: Si el circuito está abierto
        """
        with self._lock:
            self._check_state_transition()
            
            if self._state == CircuitState.OPEN:
                remaining = self._get_remaining_timeout()
                raise CircuitBreakerOpen(self.name, remaining)
        
        try:
            result = func()
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure(e)
            raise
    
    def _check_state_transition(self) -> None:
        """Verifica si debe transicionar de estado."""
        if self._state == CircuitState.OPEN:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self._config.recovery_timeout_seconds:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info(
                    "CircuitBreaker '%s': OPEN -> HALF_OPEN (testing recovery)",
                    self.name,
                )
    
    def _on_success(self) -> None:
        """Registra un éxito."""
        with self._lock:
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._state = CircuitState.CLOSED
                    self._failure_count = 0
                    logger.info(
                        "CircuitBreaker '%s': HALF_OPEN -> CLOSED (recovered)",
                        self.name,
                    )
            elif self._state == CircuitState.CLOSED:
                # Reset failure count on success
                self._failure_count = 0
    
    def _on_failure(self, error: Exception) -> None:
        """Registra un fallo."""
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._state == CircuitState.HALF_OPEN:
                # Fallo durante prueba, volver a abrir
                self._state = CircuitState.OPEN
                logger.warning(
                    "CircuitBreaker '%s': HALF_OPEN -> OPEN (test failed: %s)",
                    self.name,
                    str(error)[:100],
                )
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._config.failure_threshold:
                    self._state = CircuitState.OPEN
                    logger.warning(
                        "CircuitBreaker '%s': CLOSED -> OPEN "
                        "(failures=%d, threshold=%d, error=%s)",
                        self.name,
                        self._failure_count,
                        self._config.failure_threshold,
                        str(error)[:100],
                    )
    
    def _get_remaining_timeout(self) -> float:
        """Tiempo restante antes de probar recuperación."""
        elapsed = time.time() - self._last_failure_time
        remaining = self._config.recovery_timeout_seconds - elapsed
        return max(0.0, remaining)
    
    def reset(self) -> None:
        """Resetea el circuit breaker a estado cerrado."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            logger.info("CircuitBreaker '%s': RESET to CLOSED", self.name)
    
    def get_stats(self) -> dict:
        """Estadísticas del circuit breaker."""
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "failure_count": self._failure_count,
                "success_count": self._success_count,
                "last_failure_time": self._last_failure_time,
                "config": {
                    "failure_threshold": self._config.failure_threshold,
                    "recovery_timeout_seconds": self._config.recovery_timeout_seconds,
                    "success_threshold": self._config.success_threshold,
                },
            }


# Re-exportar utilidades desde módulo separado
from .circuit_breaker_utils import (
    circuit_breaker_decorator as circuit_breaker,
    get_db_circuit_breaker,
    reset_db_circuit_breaker,
)

"""Retry con backoff exponencial.

Proporciona decorador y utilidades para reintentar operaciones fallidas.
"""

from __future__ import annotations

import functools
import logging
import random
import time
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, Type, TypeVar, Union

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class RetryConfig:
    """Configuración para retry con backoff."""
    
    max_attempts: int = 3
    base_delay: float = 0.5  # segundos
    max_delay: float = 10.0  # segundos
    exponential_base: float = 2.0
    jitter: bool = True  # Añadir variación aleatoria
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,)
    
    def calculate_delay(self, attempt: int) -> float:
        """Calcula el delay para un intento dado.
        
        Args:
            attempt: Número de intento (1-indexed)
            
        Returns:
            Delay en segundos
        """
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        delay = min(delay, self.max_delay)
        
        if self.jitter:
            # Añadir jitter de ±25%
            jitter_range = delay * 0.25
            delay += random.uniform(-jitter_range, jitter_range)
        
        return max(0, delay)


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    max_attempts: int = 3,
    base_delay: float = 0.5,
    max_delay: float = 10.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[int, Exception], None]] = None,
):
    """Decorador para retry con backoff exponencial.
    
    Args:
        config: Configuración de retry (si se proporciona, ignora otros args)
        max_attempts: Número máximo de intentos
        base_delay: Delay base en segundos
        max_delay: Delay máximo en segundos
        retryable_exceptions: Tupla de excepciones que disparan retry
        on_retry: Callback llamado antes de cada retry (attempt, exception)
        
    Returns:
        Decorador
        
    Example:
        @retry_with_backoff(max_attempts=3, base_delay=0.5)
        def call_database():
            ...
    """
    if config is None:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            retryable_exceptions=retryable_exceptions,
        )
    
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> T:
            last_exception: Optional[Exception] = None
            
            for attempt in range(1, config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                    
                except config.retryable_exceptions as e:
                    last_exception = e
                    
                    if attempt == config.max_attempts:
                        # Último intento, propagar excepción
                        logger.error(
                            "RETRY_EXHAUSTED func=%s attempts=%d err=%s",
                            func.__name__, attempt, e
                        )
                        raise
                    
                    delay = config.calculate_delay(attempt)
                    
                    logger.warning(
                        "RETRY func=%s attempt=%d/%d delay=%.2fs err=%s",
                        func.__name__, attempt, config.max_attempts, delay, e
                    )
                    
                    if on_retry:
                        on_retry(attempt, e)
                    
                    time.sleep(delay)
            
            # No debería llegar aquí, pero por seguridad
            if last_exception:
                raise last_exception
            raise RuntimeError("Retry loop completed without result")
        
        return wrapper
    return decorator


class RetryExecutor:
    """Ejecutor de operaciones con retry.
    
    Alternativa al decorador para casos donde se necesita más control.
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self._config = config or RetryConfig()
        self._total_attempts = 0
        self._total_retries = 0
        self._total_failures = 0
    
    @property
    def stats(self) -> dict:
        """Estadísticas del ejecutor."""
        return {
            "total_attempts": self._total_attempts,
            "total_retries": self._total_retries,
            "total_failures": self._total_failures,
        }
    
    def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs,
    ) -> T:
        """Ejecuta una función con retry.
        
        Args:
            func: Función a ejecutar
            *args: Argumentos posicionales
            **kwargs: Argumentos con nombre
            
        Returns:
            Resultado de la función
            
        Raises:
            La última excepción si se agotan los reintentos
        """
        last_exception: Optional[Exception] = None
        
        for attempt in range(1, self._config.max_attempts + 1):
            self._total_attempts += 1
            
            try:
                return func(*args, **kwargs)
                
            except self._config.retryable_exceptions as e:
                last_exception = e
                
                if attempt == self._config.max_attempts:
                    self._total_failures += 1
                    raise
                
                self._total_retries += 1
                delay = self._config.calculate_delay(attempt)
                time.sleep(delay)
        
        if last_exception:
            raise last_exception
        raise RuntimeError("Retry loop completed without result")

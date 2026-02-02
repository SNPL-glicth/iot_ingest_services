"""Utilidades y singleton para Circuit Breaker.

Extraído de circuit_breaker.py para mantener archivos <150 líneas.
"""

from __future__ import annotations

from typing import Callable, TypeVar, Optional
from functools import wraps

T = TypeVar("T")


def circuit_breaker_decorator(cb: "CircuitBreaker"):
    """Decorador para proteger funciones con circuit breaker.
    
    Uso:
        db_cb = CircuitBreaker("database")
        
        @circuit_breaker_decorator(db_cb)
        def query_database():
            return db.execute(...)
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args, **kwargs) -> T:
            return cb.call(lambda: func(*args, **kwargs))
        return wrapper
    return decorator


# Singleton para BD
_db_circuit_breaker: Optional["CircuitBreaker"] = None


def get_db_circuit_breaker() -> "CircuitBreaker":
    """Obtiene el circuit breaker singleton para la BD."""
    global _db_circuit_breaker
    if _db_circuit_breaker is None:
        from .circuit_breaker import CircuitBreaker
        _db_circuit_breaker = CircuitBreaker("database")
    return _db_circuit_breaker


def reset_db_circuit_breaker() -> None:
    """Resetea el circuit breaker de BD."""
    global _db_circuit_breaker
    if _db_circuit_breaker:
        _db_circuit_breaker.reset()


# Import para type hints
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .circuit_breaker import CircuitBreaker

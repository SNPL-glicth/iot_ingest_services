"""Configuración y modelos para Circuit Breaker.

Extraído de circuit_breaker.py para mantener archivos <150 líneas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum


class CircuitState(str, Enum):
    """Estados del circuit breaker."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    """Configuración del circuit breaker."""
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 30.0
    success_threshold: int = 2
    
    @classmethod
    def from_env(cls) -> "CircuitBreakerConfig":
        return cls(
            failure_threshold=int(os.getenv("CB_FAILURE_THRESHOLD", "5")),
            recovery_timeout_seconds=float(os.getenv("CB_RECOVERY_TIMEOUT", "30")),
            success_threshold=int(os.getenv("CB_SUCCESS_THRESHOLD", "2")),
        )


class CircuitBreakerOpen(Exception):
    """Excepción cuando el circuito está abierto."""
    
    def __init__(self, name: str, remaining_seconds: float):
        self.name = name
        self.remaining_seconds = remaining_seconds
        super().__init__(
            f"Circuit breaker '{name}' is OPEN. "
            f"Retry in {remaining_seconds:.1f}s"
        )

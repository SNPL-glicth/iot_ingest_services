"""Módulo de resiliencia para ingesta.

Contiene:
- MessageDeduplicator: Deduplicación de mensajes con Redis
- DeadLetterQueue: Cola de mensajes fallidos
- retry_with_backoff: Decorador para retry con backoff exponencial
- Factory functions: get_deduplicator, get_dlq, get_resilience_components
"""

from .deduplication import MessageDeduplicator
from .dead_letter import DeadLetterQueue
from .retry import retry_with_backoff, RetryConfig
from .factory import (
    get_deduplicator,
    get_dlq,
    get_resilience_components,
    get_resilience_health,
    reset_resilience,
)
from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpen,
    CircuitState,
    get_db_circuit_breaker,
    reset_db_circuit_breaker,
    circuit_breaker,
)
from .dlq_startup import (
    start_dlq_consumer,
    stop_dlq_consumer,
    get_dlq_consumer,
    get_dlq_consumer_stats,
)

__all__ = [
    "MessageDeduplicator",
    "DeadLetterQueue",
    "retry_with_backoff",
    "RetryConfig",
    "get_deduplicator",
    "get_dlq",
    "get_resilience_components",
    "get_resilience_health",
    "reset_resilience",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerOpen",
    "CircuitState",
    "get_db_circuit_breaker",
    "reset_db_circuit_breaker",
    "circuit_breaker",
    "start_dlq_consumer",
    "stop_dlq_consumer",
    "get_dlq_consumer",
    "get_dlq_consumer_stats",
]

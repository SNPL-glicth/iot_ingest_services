"""Models and configuration for reading router.

Extracted from router.py for modularity.
"""

from __future__ import annotations

from enum import Enum
from sqlalchemy.exc import OperationalError, DBAPIError

from .resilience import RetryConfig


class PipelineType(Enum):
    """Tipo de pipeline al que pertenece una lectura."""
    ALERT = "alert"
    WARNING = "warning"
    PREDICTION = "prediction"
    SP_HANDLED = "sp_handled"


# Configuración de retry para el SP
SP_RETRY_CONFIG = RetryConfig(
    max_attempts=3,
    base_delay=0.5,
    max_delay=5.0,
    retryable_exceptions=(OperationalError, DBAPIError),
)

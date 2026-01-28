"""Factory para crear instancias del broker.

Centraliza la configuración y creación del broker.
"""

from __future__ import annotations

import os
from typing import Optional

from iot_machine_learning.ml_service.reading_broker import ReadingBroker
from iot_machine_learning.ml_service.in_memory_broker import InMemoryReadingBroker

from .throttled import ThrottledReadingBroker


_broker_instance: Optional[ReadingBroker] = None


def create_broker(min_interval_seconds: Optional[float] = None) -> ReadingBroker:
    """Crea una nueva instancia del broker.
    
    Args:
        min_interval_seconds: Intervalo mínimo entre publicaciones por sensor.
                              Default: ML_PUBLISH_MIN_INTERVAL_SECONDS env var o 1.0
    
    Returns:
        ReadingBroker configurado con throttling
    """
    if min_interval_seconds is None:
        min_interval_seconds = float(
            os.getenv("ML_PUBLISH_MIN_INTERVAL_SECONDS", "1.0")
        )
    
    return ThrottledReadingBroker(
        InMemoryReadingBroker(),
        min_interval_seconds=min_interval_seconds,
    )


def get_broker() -> ReadingBroker:
    """Obtiene la instancia singleton del broker.
    
    Crea el broker en la primera llamada y lo reutiliza después.
    """
    global _broker_instance
    if _broker_instance is None:
        _broker_instance = create_broker()
    return _broker_instance


def reset_broker() -> None:
    """Resetea el broker singleton (útil para testing)."""
    global _broker_instance
    _broker_instance = None

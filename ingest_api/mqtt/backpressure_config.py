"""Configuración y modelos para Backpressure Queue.

Extraído de backpressure.py para mantener archivos <150 líneas.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class BackpressureConfig:
    """Configuración de backpressure."""
    max_queue_size: int = 10000
    rate_limit_per_sec: float = 0.0  # 0 = sin límite
    drop_oldest: bool = True  # True = drop oldest, False = drop newest
    
    @classmethod
    def from_env(cls) -> "BackpressureConfig":
        return cls(
            max_queue_size=int(os.getenv("MQTT_QUEUE_MAX_SIZE", "10000")),
            rate_limit_per_sec=float(os.getenv("MQTT_RATE_LIMIT_PER_SEC", "0")),
            drop_oldest=os.getenv("MQTT_DROP_OLDEST", "true").lower() == "true",
        )


@dataclass
class BackpressureStats:
    """Estadísticas de backpressure."""
    enqueued: int = 0
    dequeued: int = 0
    dropped: int = 0
    current_size: int = 0
    max_size: int = 0
    rate_limited: int = 0

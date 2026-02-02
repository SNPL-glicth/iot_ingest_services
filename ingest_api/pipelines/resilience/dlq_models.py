"""Modelos y configuración para DLQ Consumer.

Extraído de dlq_consumer.py para mantener archivos <150 líneas.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class DLQConsumerConfig:
    """Configuración del consumer DLQ."""
    batch_size: int = 10
    interval_seconds: float = 60.0
    max_retries: int = 3
    archive_stream: str = "dlq:archive"
    
    @classmethod
    def from_env(cls) -> "DLQConsumerConfig":
        return cls(
            batch_size=int(os.getenv("DLQ_CONSUMER_BATCH_SIZE", "10")),
            interval_seconds=float(os.getenv("DLQ_CONSUMER_INTERVAL_SEC", "60")),
            max_retries=int(os.getenv("DLQ_MAX_RETRIES", "3")),
            archive_stream=os.getenv("DLQ_ARCHIVE_STREAM", "dlq:archive"),
        )


@dataclass
class DLQMessage:
    """Mensaje de la DLQ."""
    msg_id: str
    payload: Any
    error: str
    error_type: str
    source: str
    timestamp: float
    retry_count: int = 0
    
    @classmethod
    def from_redis(cls, msg_id: str, data: Dict[str, Any]) -> "DLQMessage":
        """Crea un mensaje desde datos de Redis."""
        return cls(
            msg_id=msg_id,
            payload=data.get("payload"),
            error=data.get("error", "unknown"),
            error_type=data.get("error_type", "unknown"),
            source=data.get("source", "unknown"),
            timestamp=float(data.get("timestamp", time.time())),
            retry_count=int(data.get("retry_count", 0)),
        )


@dataclass
class DLQConsumerStats:
    """Estadísticas del consumer."""
    messages_processed: int = 0
    messages_recovered: int = 0
    messages_archived: int = 0
    messages_failed: int = 0
    last_run_at: Optional[float] = None
    last_batch_size: int = 0

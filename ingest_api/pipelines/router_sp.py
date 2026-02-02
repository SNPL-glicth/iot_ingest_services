"""SP execution with retry for reading router.

Extracted from router.py for modularity.
"""

from __future__ import annotations

import logging
import time
from typing import Optional
from datetime import datetime

from sqlalchemy.orm import Session
from sqlalchemy import text

from .router_models import SP_RETRY_CONFIG

logger = logging.getLogger(__name__)


def execute_sp_with_retry(
    db: Session,
    sensor_id: int,
    value: float,
    device_timestamp: Optional[datetime],
) -> None:
    """Ejecuta el SP con retry y backoff exponencial."""
    last_error: Optional[Exception] = None
    
    for attempt in range(1, SP_RETRY_CONFIG.max_attempts + 1):
        try:
            db.execute(
                text(
                    """
                    EXEC sp_insert_reading_and_check_threshold 
                        @p_sensor_id = :sensor_id, 
                        @p_value = :value,
                        @p_device_timestamp = :device_ts
                    """
                ),
                {
                    "sensor_id": sensor_id,
                    "value": float(value),
                    "device_ts": device_timestamp,
                },
            )
            return  # Éxito
            
        except SP_RETRY_CONFIG.retryable_exceptions as e:
            last_error = e
            
            if attempt == SP_RETRY_CONFIG.max_attempts:
                logger.error(
                    "SP_RETRY_EXHAUSTED sensor_id=%s attempts=%d err=%s",
                    sensor_id, attempt, e
                )
                raise
            
            delay = SP_RETRY_CONFIG.calculate_delay(attempt)
            logger.warning(
                "SP_RETRY sensor_id=%s attempt=%d/%d delay=%.2fs err=%s",
                sensor_id, attempt, SP_RETRY_CONFIG.max_attempts, delay, e
            )
            time.sleep(delay)
    
    if last_error:
        raise last_error


def get_sensor_type_cached(
    db: Session,
    sensor_id: int,
    cache: dict,
) -> str:
    """Obtiene el tipo de sensor desde cache o BD."""
    if sensor_id in cache:
        return cache[sensor_id]
    
    try:
        result = db.execute(
            text("SELECT sensor_type FROM dbo.sensors WHERE id = :id"),
            {"id": sensor_id}
        ).fetchone()
        sensor_type = result[0] if result and result[0] else "unknown"
        cache[sensor_id] = sensor_type
        return sensor_type
    except Exception:
        return "unknown"

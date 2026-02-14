"""Módulo de procesamiento de lecturas MQTT.

Procesa lecturas usando el SP sp_insert_reading_and_check_threshold.
Circuit breaker (E-3 / RC-4 fix) protege contra fallos en cascada de SQL.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .validators import MQTTReadingPayload
from .connections import RedisConnection

logger = logging.getLogger(__name__)


class ReadingProcessor:
    """Procesa lecturas MQTT usando el SP de dominio.

    Integra circuit breaker: si SQL falla N veces consecutivas, las
    lecturas se envían a DLQ en lugar de bloquear el paho thread.
    """

    def __init__(
        self,
        engine: Engine,
        redis_conn: Optional[RedisConnection] = None,
        circuit_breaker=None,
        dlq=None,
    ):
        self._engine = engine
        self._redis = redis_conn
        self._cb = circuit_breaker
        self._dlq = dlq

    def process(self, payload: MQTTReadingPayload) -> bool:
        """Procesa una lectura usando el SP.

        Returns:
            True si se procesó correctamente, False en caso contrario.
        """
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            logger.warning("[PROCESSOR] Invalid sensor_id: %s", payload.sensor_id)
            return False

        device_timestamp = payload.timestamp_datetime

        if self._cb is not None:
            return self._process_with_cb(payload, sensor_id, device_timestamp)
        return self._process_direct(payload, sensor_id, device_timestamp)

    # ------------------------------------------------------------------
    # Internal processing paths
    # ------------------------------------------------------------------

    def _process_direct(self, payload, sensor_id, device_ts) -> bool:
        """Original path — no circuit breaker."""
        try:
            t0 = time.monotonic()
            self._execute_sp(sensor_id, payload.value, device_ts)
            self._publish_to_ml(payload)
            logger.debug("[PROCESSOR] OK sensor=%d ms=%.1f", sensor_id,
                         (time.monotonic() - t0) * 1000)
            return True
        except Exception as e:
            logger.error("[PROCESSOR] Failed: sensor_id=%d error=%s", sensor_id, e)
            raise

    def _process_with_cb(self, payload, sensor_id, device_ts) -> bool:
        """Circuit-breaker-protected path."""
        from ..pipelines.resilience.circuit_breaker_config import CircuitBreakerOpen

        try:
            self._cb.call(
                lambda: self._execute_sp(sensor_id, payload.value, device_ts),
            )
            self._publish_to_ml(payload)
            return True
        except CircuitBreakerOpen:
            self._send_to_dlq(payload, sensor_id, "circuit_breaker_open")
            return False
        except Exception as e:
            self._send_to_dlq(payload, sensor_id, str(e)[:200])
            logger.error("[PROCESSOR] CB pass-through fail: sensor=%d err=%s",
                         sensor_id, e)
            return False

    def _send_to_dlq(self, payload, sensor_id, error_msg: str) -> None:
        """Best-effort enqueue to DLQ."""
        if self._dlq is None:
            logger.warning("[PROCESSOR] DLQ unavailable, dropping sensor=%d", sensor_id)
            return
        try:
            self._dlq.send(
                payload=payload.model_dump() if hasattr(payload, "model_dump") else payload.dict(),
                error=error_msg,
                error_type="sp_circuit_breaker",
                source="mqtt_processor",
                sensor_id=sensor_id,
            )
        except Exception as exc:
            logger.error("[PROCESSOR] DLQ send failed: %s", exc)
    
    def _execute_sp(self, sensor_id: int, value: float, device_ts: Optional[datetime]):
        """Ejecuta el SP de inserción y evaluación."""
        with self._engine.begin() as conn:
            conn.execute(
                text("""
                    EXEC sp_insert_reading_and_check_threshold 
                        @p_sensor_id = :sensor_id, 
                        @p_value = :value,
                        @p_device_timestamp = :device_ts
                """),
                {
                    "sensor_id": sensor_id,
                    "value": float(value),
                    "device_ts": device_ts,
                },
            )
    
    def _publish_to_ml(self, payload: MQTTReadingPayload):
        """Publica lectura a Redis Stream para ML."""
        if not self._redis or not self._redis.is_connected:
            return
        
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            return
        
        data = {
            "sensor_id": str(sensor_id),
            "sensor_type": payload.sensor_type or "unknown",
            "value": str(payload.value),
            "timestamp": str(payload.timestamp_float),
            "timestamp_iso": payload.timestamp,
        }
        
        if self._redis.publish(data):
            logger.debug(
                "[PROCESSOR] Published to ML: sensor_id=%d value=%.4f",
                sensor_id,
                payload.value,
            )

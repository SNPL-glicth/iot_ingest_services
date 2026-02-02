"""Módulo de procesamiento de lecturas MQTT.

Procesa lecturas usando el SP sp_insert_reading_and_check_threshold.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .validators import MQTTReadingPayload
from .connections import RedisConnection

logger = logging.getLogger(__name__)


class ReadingProcessor:
    """Procesa lecturas MQTT usando el SP de dominio."""
    
    def __init__(self, engine: Engine, redis_conn: Optional[RedisConnection] = None):
        self._engine = engine
        self._redis = redis_conn
    
    def process(self, payload: MQTTReadingPayload) -> bool:
        """Procesa una lectura usando el SP.
        
        El SP maneja TODO el pipeline de dominio:
        - INSERT en sensor_readings
        - Evaluación de umbrales (warning/critical)
        - Creación de alerts
        - Creación de alert_notifications
        - Detección de delta spike
        - Creación de ml_events
        
        Returns:
            True si se procesó correctamente, False en caso contrario.
        """
        sensor_id = payload.sensor_id_int
        if sensor_id is None:
            logger.warning("[PROCESSOR] Invalid sensor_id: %s", payload.sensor_id)
            return False
        
        device_timestamp = self._parse_timestamp(payload.timestamp)
        
        try:
            self._execute_sp(sensor_id, payload.value, device_timestamp)
            self._publish_to_ml(payload)
            
            logger.debug(
                "[PROCESSOR] Processed: sensor_id=%d value=%.4f",
                sensor_id,
                payload.value,
            )
            return True
            
        except Exception as e:
            logger.error("[PROCESSOR] Failed: sensor_id=%d error=%s", sensor_id, e)
            raise
    
    def _parse_timestamp(self, timestamp_str: str) -> Optional[datetime]:
        """Parsea timestamp ISO8601 a datetime."""
        try:
            return datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
        except Exception:
            return None
    
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
        }
        
        if self._redis.publish(data):
            logger.debug(
                "[PROCESSOR] Published to ML: sensor_id=%d value=%.4f",
                sensor_id,
                payload.value,
            )

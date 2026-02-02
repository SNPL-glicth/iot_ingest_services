"""Message handling logic for MQTT receiver.

Extracted from receiver.py for modularity.
"""

from __future__ import annotations

import logging
from typing import Optional

try:
    import orjson
    ORJSON_AVAILABLE = True
except ImportError:
    import json
    ORJSON_AVAILABLE = False

from .validators import validate_mqtt_reading
from .receiver_stats import ReceiverStats
from ..pipelines.resilience import get_deduplicator, get_dlq

logger = logging.getLogger(__name__)


def parse_json(payload: bytes, topic: str, stats: ReceiverStats) -> Optional[dict]:
    """Parsea payload JSON usando orjson si está disponible."""
    try:
        if ORJSON_AVAILABLE:
            return orjson.loads(payload)
        else:
            return json.loads(payload.decode("utf-8"))
    except Exception as e:
        logger.warning("[MQTT] Invalid JSON: %s (topic=%s)", e, topic)
        stats.failed += 1
        return None


def handle_message(msg, stats: ReceiverStats, processor) -> None:
    """Procesa un mensaje MQTT recibido."""
    stats.received += 1
    
    import time
    stats.last_message_at = time.time()
    
    try:
        data = parse_json(msg.payload, msg.topic, stats)
        if data is None:
            _send_to_dlq(msg.payload, "Invalid JSON", "parse_error")
            return
        
        validation = validate_mqtt_reading(data)
        if not validation.valid:
            logger.warning("[MQTT] Validation failed: %s", validation.error)
            stats.failed += 1
            _send_to_dlq(data, validation.error, "validation_error")
            return
        
        # Deduplicación
        dedup = get_deduplicator()
        if dedup:
            msg_id = validation.payload.msg_id or dedup.generate_msg_id(
                sensor_id=validation.payload.sensor_id_int or 0,
                timestamp=validation.payload.timestamp_float,
                value=validation.payload.value,
            )
            if dedup.is_duplicate(msg_id):
                stats.duplicates += 1
                logger.debug("[MQTT] Duplicate msg_id=%s", msg_id)
                return
        
        processor.process(validation.payload)
        stats.processed += 1
        
        if stats.processed % 100 == 0:
            logger.info("[MQTT] %s", stats)
        
    except Exception as e:
        logger.exception("[MQTT] Processing error: %s", e)
        stats.failed += 1
        _send_to_dlq(str(msg.payload)[:1000], str(e), "processing_error")


def _send_to_dlq(payload, error: str, error_type: str) -> None:
    """Envía mensaje a la DLQ."""
    dlq = get_dlq()
    if dlq:
        dlq.send(
            payload=payload,
            error=error,
            error_type=error_type,
            source="mqtt",
        )

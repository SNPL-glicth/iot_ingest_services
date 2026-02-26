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


def handle_message(msg, stats: ReceiverStats, processor, async_processor=None) -> None:
    """Procesa un mensaje MQTT recibido.

    If async_processor is provided, validated payloads are enqueued
    for async processing (frees paho thread). Otherwise falls back
    to synchronous processor.process().
    
    Topic routing:
    - iot/sensors/+/readings → IoT processing (existing)
    - {domain}/{source}/{stream}/data → Universal processing (new)
    """
    stats.received += 1
    
    import time
    stats.last_message_at = time.time()
    
    # Detect topic format
    topic_parts = msg.topic.split("/")
    
    # Multi-domain topic: {domain}/{source}/{stream}/data
    if len(topic_parts) == 4 and topic_parts[3] == "data":
        _handle_universal_message(msg, stats, topic_parts)
        return
    
    # IoT topic: iot/sensors/{id}/readings (existing flow - NO CHANGES)
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
        
        # Async path: enqueue and return immediately (frees paho thread)
        if async_processor is not None:
            if async_processor.enqueue(validation.payload):
                stats.processed += 1
            else:
                stats.failed += 1
        else:
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


def _handle_universal_message(msg, stats: ReceiverStats, topic_parts: list) -> None:
    """Handle multi-domain MQTT message.
    
    Topic format: {domain}/{source_id}/{stream_id}/data
    Payload: {"value": float, "timestamp": ISO8601 (optional), "metadata": dict (optional)}
    """
    domain, source_id, stream_id = topic_parts[0], topic_parts[1], topic_parts[2]
    
    # Block IoT domain - must use legacy topic
    if domain.lower() == "iot":
        logger.warning("[MQTT] Rejected: domain='iot' not allowed in universal topics")
        stats.failed += 1
        _send_to_dlq(msg.payload, "domain='iot' not allowed", "invalid_domain")
        return
    
    try:
        data = parse_json(msg.payload, msg.topic, stats)
        if data is None:
            return
        
        # Extract fields
        value = data.get("value")
        if value is None:
            logger.warning("[MQTT] Missing 'value' field in universal message")
            stats.failed += 1
            _send_to_dlq(data, "Missing 'value' field", "validation_error")
            return
        
        # Parse timestamp
        from datetime import datetime
        timestamp_str = data.get("timestamp")
        if timestamp_str:
            try:
                timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            except Exception:
                timestamp = datetime.utcnow()
        else:
            timestamp = datetime.utcnow()
        
        metadata = data.get("metadata", {})
        sequence = data.get("sequence")
        
        # Create DataPoint
        from ..core.domain.data_point import DataPoint
        dp = DataPoint(
            source_id=source_id,
            stream_id=stream_id,
            value=float(value),
            timestamp=timestamp,
            domain=domain,
            sequence=sequence,
            domain_metadata=metadata,
        )
        
        # Classify and persist
        from ..core.classification.universal_classifier import UniversalClassifier
        from ..classification.stream_config_repository import StreamConfigRepository
        from ..infrastructure.persistence import DomainPersistenceRouter, get_postgres_engine
        from ...common.db import get_engine
        
        postgres = get_postgres_engine()
        if postgres is None:
            logger.warning("[MQTT] PostgreSQL not configured - cannot process universal message")
            stats.failed += 1
            return
        
        config_repo = StreamConfigRepository(postgres)
        classifier = UniversalClassifier(config_repo)
        db_router = DomainPersistenceRouter(get_engine(), postgres)
        
        result = classifier.classify(dp)
        
        if result.should_persist:
            db_router.save_data_point(dp)
            
            if result.requires_alert:
                db_router.save_alert(dp, result)
            
            stats.processed += 1
            logger.debug(
                "[MQTT] Universal message processed: domain=%s source=%s stream=%s",
                domain, source_id, stream_id,
            )
        else:
            stats.failed += 1
            logger.warning(
                "[MQTT] Universal message rejected: %s",
                result.reason,
            )
        
    except Exception as e:
        logger.exception("[MQTT] Universal message processing error: %s", e)
        stats.failed += 1
        _send_to_dlq(str(msg.payload)[:1000], str(e), "processing_error")

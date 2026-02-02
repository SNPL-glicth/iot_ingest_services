"""DLQ Consumer startup integration.

Provides functions to start/stop DLQ consumer with the application lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Optional, Any

from .dlq_consumer import DLQConsumer, DLQMessage
from .factory import get_redis_client

logger = logging.getLogger(__name__)

# Singleton consumer
_dlq_consumer: Optional[DLQConsumer] = None


async def default_reprocess_handler(msg: DLQMessage) -> bool:
    """Default handler that logs and discards old messages.
    
    In production, this should be replaced with actual reprocessing logic.
    """
    import time
    
    # Discard messages older than 1 hour
    if msg.created_at and (time.time() - msg.created_at) > 3600:
        logger.info("DLQ: Discarding old message: %s", msg.msg_id)
        return True  # Mark as processed (discard)
    
    # Log for manual review
    logger.warning(
        "DLQ: Message needs manual review: msg_id=%s error_type=%s source=%s",
        msg.msg_id, msg.error_type, msg.source
    )
    
    # Return False to keep in queue for retry
    return False


async def start_dlq_consumer() -> bool:
    """Start the DLQ consumer.
    
    Call this during application startup.
    
    Returns:
        True if started successfully, False otherwise.
    """
    global _dlq_consumer
    
    if _dlq_consumer is not None:
        return _dlq_consumer._running
    
    try:
        redis_client = get_redis_client()
        if redis_client is None:
            logger.warning("DLQ consumer not started: Redis not available")
            return False
        
        _dlq_consumer = DLQConsumer(redis_client)
        _dlq_consumer.set_handler(default_reprocess_handler)
        await _dlq_consumer.start()
        
        logger.info("DLQ consumer started successfully")
        return True
        
    except Exception as e:
        logger.exception("Failed to start DLQ consumer: %s", e)
        return False


async def stop_dlq_consumer() -> None:
    """Stop the DLQ consumer.
    
    Call this during application shutdown.
    """
    global _dlq_consumer
    
    if _dlq_consumer is not None:
        await _dlq_consumer.stop()
        _dlq_consumer = None
        logger.info("DLQ consumer stopped")


def get_dlq_consumer() -> Optional[DLQConsumer]:
    """Get the DLQ consumer singleton."""
    return _dlq_consumer


def get_dlq_consumer_stats() -> dict:
    """Get DLQ consumer statistics."""
    if _dlq_consumer is None:
        return {"running": False, "error": "Consumer not initialized"}
    return _dlq_consumer.get_stats()

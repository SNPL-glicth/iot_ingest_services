"""Operaciones Redis para DLQ Consumer.

Extraído de dlq_consumer.py para mantener archivos <150 líneas.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, List

from .dlq_models import DLQMessage, DLQConsumerConfig

logger = logging.getLogger(__name__)


async def read_dlq_messages(
    redis_client: Any,
    dlq_stream: str,
    batch_size: int,
) -> List[DLQMessage]:
    """Lee mensajes de la DLQ."""
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: redis_client.xrange(dlq_stream, count=batch_size),
        )
        
        messages = []
        for msg_id, data in result:
            msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
            data_decoded = {
                (k.decode() if isinstance(k, bytes) else k): 
                (v.decode() if isinstance(v, bytes) else v)
                for k, v in data.items()
            }
            messages.append(DLQMessage.from_redis(msg_id_str, data_decoded))
        
        return messages
        
    except Exception as e:
        logger.warning("Failed to read DLQ: %s", e)
        return []


async def delete_dlq_message(
    redis_client: Any,
    dlq_stream: str,
    msg_id: str,
) -> None:
    """Elimina un mensaje de la DLQ."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: redis_client.xdel(dlq_stream, msg_id),
        )
    except Exception as e:
        logger.warning("Failed to delete DLQ message %s: %s", msg_id, e)


async def archive_dlq_message(
    redis_client: Any,
    dlq_stream: str,
    archive_stream: str,
    msg: DLQMessage,
) -> None:
    """Archiva un mensaje irrecuperable."""
    try:
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: redis_client.xadd(
                archive_stream,
                {
                    "original_id": msg.msg_id,
                    "payload": str(msg.payload)[:1000],
                    "error": msg.error,
                    "error_type": msg.error_type,
                    "source": msg.source,
                    "original_timestamp": str(msg.timestamp),
                    "archived_at": str(time.time()),
                    "retry_count": str(msg.retry_count),
                },
                maxlen=10000,
            ),
        )
        
        await delete_dlq_message(redis_client, dlq_stream, msg.msg_id)
        
    except Exception as e:
        logger.warning("Failed to archive DLQ message %s: %s", msg.msg_id, e)


async def update_dlq_retry_count(
    redis_client: Any,
    dlq_stream: str,
    msg: DLQMessage,
) -> None:
    """Actualiza el contador de reintentos."""
    try:
        await delete_dlq_message(redis_client, dlq_stream, msg.msg_id)
        
        await asyncio.get_event_loop().run_in_executor(
            None,
            lambda: redis_client.xadd(
                dlq_stream,
                {
                    "payload": str(msg.payload)[:1000],
                    "error": msg.error,
                    "error_type": msg.error_type,
                    "source": msg.source,
                    "timestamp": str(msg.timestamp),
                    "retry_count": str(msg.retry_count + 1),
                },
            ),
        )
    except Exception as e:
        logger.warning("Failed to update retry count for %s: %s", msg.msg_id, e)

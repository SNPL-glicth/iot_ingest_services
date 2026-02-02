"""Consumer para Dead Letter Queue.

FIX 2026-02-02: Implementa consumer para procesar mensajes fallidos (Sprint 3).
Refactorizado: Modelos extraídos a dlq_models.py para mantener <150 líneas.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional, Callable, Awaitable, Any, List

from .dlq_models import DLQConsumerConfig, DLQMessage, DLQConsumerStats
from .dlq_operations import (
    read_dlq_messages,
    delete_dlq_message,
    archive_dlq_message,
    update_dlq_retry_count,
)

logger = logging.getLogger(__name__)

# Tipo para handler de reprocesamiento
ReprocessHandler = Callable[[DLQMessage], Awaitable[bool]]


class DLQConsumer:
    """Consumer para procesar mensajes de la Dead Letter Queue.
    
    Uso:
        consumer = DLQConsumer(redis_client)
        
        async def reprocess(msg: DLQMessage) -> bool:
            # Intentar reprocesar
            return success
        
        consumer.set_handler(reprocess)
        await consumer.start()
    """
    
    def __init__(
        self,
        redis_client: Any,
        config: Optional[DLQConsumerConfig] = None,
        dlq_stream: str = "dlq:ingest",
    ):
        self._redis = redis_client
        self._config = config or DLQConsumerConfig.from_env()
        self._dlq_stream = dlq_stream
        
        self._handler: Optional[ReprocessHandler] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._stats = DLQConsumerStats()
        
        logger.info(
            "DLQConsumer initialized: stream=%s, batch_size=%d, interval=%.1fs",
            dlq_stream,
            self._config.batch_size,
            self._config.interval_seconds,
        )
    
    def set_handler(self, handler: ReprocessHandler) -> None:
        """Configura el handler de reprocesamiento."""
        self._handler = handler
    
    async def start(self) -> None:
        """Inicia el consumer en background."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("DLQConsumer started")
    
    async def stop(self) -> None:
        """Detiene el consumer."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("DLQConsumer stopped")
    
    async def _run_loop(self) -> None:
        """Loop principal del consumer."""
        while self._running:
            try:
                await self._process_batch()
                await asyncio.sleep(self._config.interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("DLQConsumer error: %s", e)
                await asyncio.sleep(5)  # Backoff on error
    
    async def _process_batch(self) -> None:
        """Procesa un batch de mensajes de la DLQ."""
        if not self._handler:
            logger.warning("DLQConsumer: No handler configured")
            return
        
        try:
            # Leer mensajes de la DLQ
            messages = await self._read_messages()
            if not messages:
                return
            
            self._stats.last_run_at = time.time()
            self._stats.last_batch_size = len(messages)
            
            for msg in messages:
                await self._process_message(msg)
            
            logger.info(
                "DLQConsumer batch completed: processed=%d, recovered=%d, archived=%d",
                len(messages),
                self._stats.messages_recovered,
                self._stats.messages_archived,
            )
            
        except Exception as e:
            logger.exception("DLQConsumer batch error: %s", e)
    
    async def _read_messages(self) -> List[DLQMessage]:
        """Lee mensajes de la DLQ."""
        return await read_dlq_messages(
            self._redis, self._dlq_stream, self._config.batch_size
        )
    
    async def _process_message(self, msg: DLQMessage) -> None:
        """Procesa un mensaje individual."""
        self._stats.messages_processed += 1
        
        try:
            success = await self._handler(msg)
            
            if success:
                await delete_dlq_message(self._redis, self._dlq_stream, msg.msg_id)
                self._stats.messages_recovered += 1
                logger.info("DLQ message recovered: %s", msg.msg_id)
            else:
                if msg.retry_count >= self._config.max_retries:
                    await archive_dlq_message(
                        self._redis, self._dlq_stream,
                        self._config.archive_stream, msg
                    )
                    self._stats.messages_archived += 1
                    logger.warning("DLQ message archived (max retries): %s", msg.msg_id)
                else:
                    await update_dlq_retry_count(self._redis, self._dlq_stream, msg)
                    self._stats.messages_failed += 1
                    
        except Exception as e:
            logger.exception("Failed to process DLQ message %s: %s", msg.msg_id, e)
            self._stats.messages_failed += 1
    
    def get_stats(self) -> dict:
        """Estadísticas del consumer."""
        return {
            "running": self._running,
            "messages_processed": self._stats.messages_processed,
            "messages_recovered": self._stats.messages_recovered,
            "messages_archived": self._stats.messages_archived,
            "messages_failed": self._stats.messages_failed,
            "last_run_at": self._stats.last_run_at,
            "last_batch_size": self._stats.last_batch_size,
            "config": {
                "batch_size": self._config.batch_size,
                "interval_seconds": self._config.interval_seconds,
                "max_retries": self._config.max_retries,
            },
        }

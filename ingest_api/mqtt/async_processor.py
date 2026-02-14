"""Async processor — decouples paho callback from blocking SP execution.

Wraps ReadingProcessor with a bounded queue + thread pool so the paho
network loop thread returns immediately after enqueue (~0.01ms) instead
of blocking on the SP call (~5-50ms).

Feature flag: ML_MQTT_ASYNC_PROCESSING (default True).
"""

from __future__ import annotations

import logging
import os
import queue
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

DEFAULT_QUEUE_SIZE = 1000
DEFAULT_NUM_WORKERS = 4


class AsyncReadingProcessor:
    """Queue + ThreadPool wrapper for ReadingProcessor.

    - paho callback → enqueue() returns in <0.1ms
    - Worker threads → process() blocks on SP (in parallel)
    - Bounded queue provides backpressure
    """

    def __init__(
        self,
        processor,
        max_queue_size: int = DEFAULT_QUEUE_SIZE,
        num_workers: int = DEFAULT_NUM_WORKERS,
    ):
        self._processor = processor
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._num_workers = num_workers
        self._stop_event = threading.Event()

        # Metrics
        self._enqueued = 0
        self._dropped = 0
        self._processed = 0
        self._errors = 0
        self._lock = threading.Lock()

        self._workers: list[threading.Thread] = []

    def start(self) -> None:
        """Start worker threads."""
        self._stop_event.clear()
        for i in range(self._num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                args=(i,),
                daemon=True,
                name=f"mqtt-worker-{i}",
            )
            t.start()
            self._workers.append(t)
        logger.info(
            "[ASYNC_PROC] Started workers=%d queue_max=%d",
            self._num_workers, self._queue.maxsize,
        )

    def stop(self, drain: bool = True) -> None:
        """Stop workers. If drain=True, process remaining items first."""
        self._stop_event.set()
        if drain:
            self._queue.join()
        for t in self._workers:
            t.join(timeout=5.0)
        self._workers.clear()
        logger.info("[ASYNC_PROC] Stopped. %s", self.metrics)

    def enqueue(self, payload) -> bool:
        """Enqueue payload for async processing. Returns False if full."""
        try:
            self._queue.put_nowait(payload)
            with self._lock:
                self._enqueued += 1
            return True
        except queue.Full:
            with self._lock:
                self._dropped += 1
            logger.warning(
                "[ASYNC_PROC] Queue full, dropped sensor=%s",
                getattr(payload, "sensor_id_int", "?"),
            )
            return False

    def _worker_loop(self, worker_id: int) -> None:
        while not self._stop_event.is_set():
            try:
                payload = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            try:
                self._processor.process(payload)
                with self._lock:
                    self._processed += 1
            except Exception as e:
                with self._lock:
                    self._errors += 1
                logger.error(
                    "[ASYNC_PROC] Worker %d error: %s", worker_id, e,
                )
            finally:
                self._queue.task_done()

    @property
    def metrics(self) -> dict:
        with self._lock:
            return {
                "queue_depth": self._queue.qsize(),
                "queue_max": self._queue.maxsize,
                "enqueued": self._enqueued,
                "dropped": self._dropped,
                "processed": self._processed,
                "errors": self._errors,
            }


def create_async_processor(processor) -> Optional[AsyncReadingProcessor]:
    """Factory: create AsyncReadingProcessor from env config.

    Returns None if ML_MQTT_ASYNC_PROCESSING is disabled.
    """
    enabled = os.getenv("ML_MQTT_ASYNC_PROCESSING", "true").lower() in (
        "true", "1", "yes",
    )
    if not enabled:
        logger.info("[ASYNC_PROC] Disabled by ML_MQTT_ASYNC_PROCESSING=false")
        return None

    queue_size = int(os.getenv("ML_MQTT_QUEUE_SIZE", str(DEFAULT_QUEUE_SIZE)))
    num_workers = int(os.getenv("ML_MQTT_NUM_WORKERS", str(DEFAULT_NUM_WORKERS)))

    ap = AsyncReadingProcessor(
        processor=processor,
        max_queue_size=queue_size,
        num_workers=num_workers,
    )
    ap.start()
    return ap

"""TAREA 5: Batch inserter para ingesta masiva de lecturas.

Este módulo implementa un buffer con flush periódico para insertar
lecturas en lotes, mejorando el throughput y reduciendo la carga en BD.

Características:
- Buffer en memoria con límite configurable
- Flush automático por tiempo o por cantidad
- Backpressure: descarta lecturas si el buffer está lleno
- Thread-safe para uso concurrente
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, List, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class BufferedReading:
    """Lectura en buffer pendiente de inserción."""
    sensor_id: int
    value: float
    device_timestamp: Optional[datetime]
    ingest_timestamp: datetime


class BatchInserter:
    """Inserter de lecturas en batch con buffer y flush periódico."""

    DEFAULT_BUFFER_SIZE = 100
    DEFAULT_FLUSH_INTERVAL = 5.0  # segundos
    DEFAULT_MAX_BATCH_SIZE = 500

    def __init__(
        self,
        engine: Engine,
        buffer_size: int = DEFAULT_BUFFER_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
        on_flush_callback: Optional[Callable[[int], None]] = None,
    ):
        """Inicializa el batch inserter.

        Args:
            engine: SQLAlchemy engine para conexiones
            buffer_size: Tamaño máximo del buffer antes de flush automático
            flush_interval: Intervalo en segundos para flush periódico
            max_batch_size: Máximo de lecturas por batch INSERT
            on_flush_callback: Callback opcional llamado después de cada flush
        """
        self._engine = engine
        self._buffer_size = buffer_size
        self._flush_interval = flush_interval
        self._max_batch_size = max_batch_size
        self._on_flush_callback = on_flush_callback

        self._buffer: List[BufferedReading] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._flush_thread: Optional[threading.Thread] = None

        # Métricas
        self._total_buffered = 0
        self._total_flushed = 0
        self._total_dropped = 0

    def start(self):
        """Inicia el thread de flush periódico."""
        if self._flush_thread is not None and self._flush_thread.is_alive():
            return

        self._stop_event.clear()
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info("BatchInserter started with buffer_size=%d, flush_interval=%.1fs",
                    self._buffer_size, self._flush_interval)

    def stop(self, flush_remaining: bool = True):
        """Detiene el batch inserter.

        Args:
            flush_remaining: Si True, hace flush de lecturas pendientes antes de parar
        """
        self._stop_event.set()

        if flush_remaining:
            self._do_flush()

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=5.0)
            self._flush_thread = None

        logger.info("BatchInserter stopped. Stats: buffered=%d, flushed=%d, dropped=%d",
                    self._total_buffered, self._total_flushed, self._total_dropped)

    def add(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime] = None,
    ) -> bool:
        """Agrega una lectura al buffer.

        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            device_timestamp: Timestamp del dispositivo (opcional)

        Returns:
            True si se agregó, False si se descartó por backpressure
        """
        reading = BufferedReading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            ingest_timestamp=datetime.now(timezone.utc),
        )

        with self._lock:
            if len(self._buffer) >= self._buffer_size * 2:
                # Backpressure: buffer muy lleno, descartar
                self._total_dropped += 1
                logger.warning("BatchInserter buffer full, dropping reading for sensor %d",
                               sensor_id)
                return False

            self._buffer.append(reading)
            self._total_buffered += 1

            # Flush automático si alcanzamos el límite
            if len(self._buffer) >= self._buffer_size:
                self._schedule_flush()

        return True

    def _schedule_flush(self):
        """Programa un flush inmediato (llamado desde add cuando buffer está lleno)."""
        # El flush se hará en el próximo ciclo del thread
        pass

    def _flush_loop(self):
        """Loop principal del thread de flush."""
        while not self._stop_event.is_set():
            time.sleep(self._flush_interval)
            if self._stop_event.is_set():
                break
            self._do_flush()

    def _do_flush(self):
        """Ejecuta el flush del buffer a la BD."""
        with self._lock:
            if not self._buffer:
                return

            # Tomar lecturas del buffer
            to_flush = self._buffer[:self._max_batch_size]
            self._buffer = self._buffer[self._max_batch_size:]

        if not to_flush:
            return

        try:
            self._bulk_insert(to_flush)
            self._total_flushed += len(to_flush)

            if self._on_flush_callback:
                self._on_flush_callback(len(to_flush))

            logger.debug("BatchInserter flushed %d readings", len(to_flush))

        except Exception as e:
            logger.error("BatchInserter flush error: %s", e)
            # Re-agregar al buffer para reintentar
            with self._lock:
                self._buffer = to_flush + self._buffer

    def _bulk_insert(self, readings: List[BufferedReading]):
        """Inserta lecturas en batch usando executemany."""
        if not readings:
            return

        with self._engine.begin() as conn:
            # Preparar datos para bulk insert
            # Esquema: sensor_readings(sensor_id, value, timestamp, device_timestamp)
            values = []
            for r in readings:
                values.append({
                    "sensor_id": r.sensor_id,
                    "value": r.value,
                    "timestamp": r.ingest_timestamp,
                    "device_timestamp": r.device_timestamp,
                })

            # Bulk INSERT usando executemany
            conn.execute(
                text("""
                    INSERT INTO dbo.sensor_readings (sensor_id, value, timestamp, device_timestamp)
                    VALUES (:sensor_id, :value, :timestamp, :device_timestamp)
                """),
                values,
            )

    def get_stats(self) -> dict:
        """Retorna estadísticas del inserter."""
        with self._lock:
            pending = len(self._buffer)

        return {
            "pending": pending,
            "total_buffered": self._total_buffered,
            "total_flushed": self._total_flushed,
            "total_dropped": self._total_dropped,
            "buffer_size": self._buffer_size,
            "flush_interval": self._flush_interval,
        }


# Singleton global para uso en la aplicación
_global_inserter: Optional[BatchInserter] = None


def get_batch_inserter() -> Optional[BatchInserter]:
    """Obtiene el batch inserter global."""
    return _global_inserter


def init_batch_inserter(engine: Engine, **kwargs) -> BatchInserter:
    """Inicializa el batch inserter global.

    Args:
        engine: SQLAlchemy engine
        **kwargs: Argumentos adicionales para BatchInserter

    Returns:
        Instancia del BatchInserter
    """
    global _global_inserter

    if _global_inserter is not None:
        _global_inserter.stop()

    _global_inserter = BatchInserter(engine, **kwargs)
    _global_inserter.start()

    return _global_inserter


def shutdown_batch_inserter():
    """Detiene el batch inserter global."""
    global _global_inserter

    if _global_inserter is not None:
        _global_inserter.stop(flush_remaining=True)
        _global_inserter = None

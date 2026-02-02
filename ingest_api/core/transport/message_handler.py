"""Handler de mensajes MQTT."""

from __future__ import annotations

import json
import logging
import time
from typing import Optional

from ..domain.reading import Reading, ReadingStatus
from ..adapters.mqtt_adapter import MQTTAdapter
from ..pipeline.processor import ReadingProcessor
from ..monitoring.stats import Stats

logger = logging.getLogger(__name__)


class MessageHandler:
    """Maneja mensajes MQTT y los procesa a través del pipeline.
    
    Responsabilidades:
    - Parseo de JSON
    - Adaptación de contrato MQTT → Dominio
    - Delegación al procesador
    - Tracking de estadísticas
    """
    
    def __init__(
        self,
        processor: ReadingProcessor,
        adapter: Optional[MQTTAdapter] = None,
    ):
        self._processor = processor
        self._adapter = adapter or MQTTAdapter()
        self._stats = Stats()
    
    def handle(self, topic: str, payload: bytes):
        """Procesa un mensaje MQTT."""
        self._stats.received += 1
        self._stats.last_message_at = time.time()
        
        try:
            # 1. Parsear JSON
            data = self._parse_json(payload, topic)
            if data is None:
                return
            
            # 2. Adaptar a modelo de dominio
            reading = self._adapter.to_reading(data)
            if reading is None:
                self._stats.failed += 1
                return
            
            # 3. Procesar lectura
            success = self._processor.process(reading)
            
            if success:
                self._stats.processed += 1
            else:
                self._stats.failed += 1
            
            # Log periódico
            if self._stats.processed % 10 == 0:
                logger.info("[HANDLER] %s", self._stats)
                
        except Exception as e:
            logger.exception("[HANDLER] Error: %s", e)
            self._stats.failed += 1
    
    def _parse_json(self, payload: bytes, topic: str) -> Optional[dict]:
        """Parsea payload JSON."""
        try:
            return json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as e:
            logger.warning("[HANDLER] Invalid JSON: %s (topic=%s)", e, topic)
            self._stats.failed += 1
            return None
    
    @property
    def stats(self) -> Stats:
        return self._stats

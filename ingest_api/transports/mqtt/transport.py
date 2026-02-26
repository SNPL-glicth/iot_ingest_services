"""MQTT Transport - Implementación del transporte MQTT universal.

IMPORTANTE: Este transporte NO interfiere con el MQTT receiver IoT existente.
Usa un cliente MQTT separado y suscribe a topics diferentes.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Iterator

from ..base import IngestTransport
from ...core.domain.data_point import DataPoint
from ...core.domain.series_id import SeriesIdMapper

logger = logging.getLogger(__name__)


class MQTTTransport(IngestTransport):
    """Transporte MQTT para ingesta universal.
    
    Topic format: {domain}/{source_id}/{stream_id}/data
    Ejemplos:
    - infrastructure/web-01/cpu/data
    - finance/binance/btc_usdt/data
    - health/patient-123/bpm/data
    
    Payload JSON: {"value": float, "timestamp": ISO8601 (optional), "metadata": dict (optional)}
    """
    
    def __init__(self):
        """Inicializa el transporte MQTT."""
        self._started = False
        self._messages_processed = 0
        self._errors = 0
    
    def start(self) -> bool:
        """Inicia el transporte MQTT.
        
        Returns:
            True si el inicio fue exitoso
        """
        self._started = True
        return True
    
    def stop(self) -> None:
        """Detiene el transporte MQTT."""
        self._started = False
    
    def parse_message(self, raw_message: Any) -> Iterator[DataPoint]:
        """Parsea mensaje MQTT a DataPoint.
        
        Args:
            raw_message: Objeto con atributos 'topic' y 'payload'
            
        Yields:
            DataPoint object
        """
        try:
            # Extraer topic y payload
            topic = raw_message.topic if hasattr(raw_message, 'topic') else raw_message.get('topic')
            payload = raw_message.payload if hasattr(raw_message, 'payload') else raw_message.get('payload')
            
            # Parsear topic: {domain}/{source_id}/{stream_id}/data
            topic_parts = topic.split('/')
            if len(topic_parts) != 4 or topic_parts[3] != 'data':
                logger.warning(f"Invalid topic format: {topic}")
                self._errors += 1
                return
            
            domain, source_id, stream_id = topic_parts[0], topic_parts[1], topic_parts[2]
            
            # Bloquear domain='iot'
            if domain.lower() == 'iot':
                logger.warning(f"Rejected domain='iot' in universal MQTT topic: {topic}")
                self._errors += 1
                return
            
            # Parsear payload JSON
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')
            data = json.loads(payload)
            
            # Extraer campos
            value = data.get('value')
            if value is None:
                logger.warning(f"Missing 'value' in MQTT payload: {topic}")
                self._errors += 1
                return
            
            # Timestamp
            timestamp_str = data.get('timestamp')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                except Exception:
                    timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()
            
            # Metadata
            metadata = data.get('metadata', {})
            sequence = data.get('sequence')
            
            # Construir series_id
            series_id = SeriesIdMapper.build_series_id(domain, source_id, stream_id)
            
            # Crear DataPoint
            dp = DataPoint.from_series_id(
                series_id=series_id,
                value=float(value),
                timestamp=timestamp,
                metadata=metadata,
                sequence=sequence,
            )
            
            self._messages_processed += 1
            yield dp
            
        except Exception as e:
            self._errors += 1
            logger.exception(f"Error parsing MQTT message: {e}")
    
    @property
    def transport_name(self) -> str:
        """Nombre del transporte.
        
        Returns:
            "mqtt"
        """
        return "mqtt"
    
    @property
    def stats(self) -> dict:
        """Estadísticas del transporte.
        
        Returns:
            Dict con estadísticas
        """
        return {
            "transport": "mqtt",
            "started": self._started,
            "messages_processed": self._messages_processed,
            "errors": self._errors,
        }

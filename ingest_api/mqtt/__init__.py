"""MQTT Receiver para Ingesta.

Este módulo proporciona:
- Receptor MQTT que escucha lecturas de sensores
- Validación de payloads
- Procesamiento con SP de dominio
- Publicación opcional a Redis Streams para ML

Estructura modular:
- connections.py: Conexiones a BD y Redis
- processor.py: Procesamiento de lecturas con SP
- receiver.py: Receptor MQTT principal
- simple_receiver.py: Wrapper de compatibilidad
"""

from .receiver import (
    MQTTReceiver,
    get_receiver,
    start_receiver,
    stop_receiver,
)
from .simple_receiver import (
    SimpleMQTTReceiver,
    get_simple_receiver,
    start_simple_receiver,
    stop_simple_receiver,
)
from .connections import DatabaseConnection, RedisConnection
from .processor import ReadingProcessor

__all__ = [
    "MQTTReceiver",
    "get_receiver",
    "start_receiver",
    "stop_receiver",
    "SimpleMQTTReceiver",
    "get_simple_receiver",
    "start_simple_receiver",
    "stop_simple_receiver",
    "DatabaseConnection",
    "RedisConnection",
    "ReadingProcessor",
]

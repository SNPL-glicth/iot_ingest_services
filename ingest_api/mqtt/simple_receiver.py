"""Receptor MQTT simple - Wrapper de compatibilidad.

Este archivo mantiene compatibilidad con el código existente.
La implementación real está modularizada en:
- connections.py: Conexiones a BD y Redis
- processor.py: Procesamiento de lecturas con SP
- receiver.py: Receptor MQTT principal
"""

from .receiver import (
    MQTTReceiver as SimpleMQTTReceiver,
    get_receiver as get_simple_receiver,
    start_receiver as start_simple_receiver,
    stop_receiver as stop_simple_receiver,
)

__all__ = [
    "SimpleMQTTReceiver",
    "get_simple_receiver",
    "start_simple_receiver",
    "stop_simple_receiver",
]

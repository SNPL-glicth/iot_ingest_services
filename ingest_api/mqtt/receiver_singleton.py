"""Singleton management for MQTT receiver.

Extracted from receiver.py for modularity.
"""

from __future__ import annotations

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Singleton
_receiver = None


def get_receiver():
    """Obtiene el receptor singleton."""
    return _receiver


def start_receiver() -> bool:
    """Inicia el receptor."""
    global _receiver
    
    # Import here to avoid circular imports
    from .receiver import MQTTReceiver
    
    if _receiver is not None:
        return _receiver.is_running
    
    broker_host = os.getenv("MQTT_BROKER_HOST", "localhost")
    broker_port = int(os.getenv("MQTT_BROKER_PORT", "1883"))
    username = os.getenv("MQTT_USERNAME") or None
    password = os.getenv("MQTT_PASSWORD") or None
    
    _receiver = MQTTReceiver(
        broker_host=broker_host,
        broker_port=broker_port,
        username=username,
        password=password,
    )
    
    return _receiver.start()


def stop_receiver():
    """Detiene el receptor."""
    global _receiver
    
    if _receiver is not None:
        _receiver.stop()
        _receiver = None

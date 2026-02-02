"""Adapter to connect ML broker to abstract interface.

This adapter wraps the ML ReadingBroker to implement IReadingBroker.
"""

from __future__ import annotations

import logging
from typing import Optional

from ..core.domain.broker_interface import IReadingBroker, Reading

logger = logging.getLogger(__name__)

# Try to import ML broker
try:
    from iot_machine_learning.ml_service.reading_broker import (
        ReadingBroker as MLReadingBroker,
        Reading as MLReading,
    )
    ML_AVAILABLE = True
except ImportError:
    ML_AVAILABLE = False
    MLReadingBroker = None
    MLReading = None


class MLBrokerAdapter(IReadingBroker):
    """Adapter that wraps ML ReadingBroker to implement IReadingBroker."""
    
    def __init__(self, ml_broker: Optional[object] = None):
        self._ml_broker = ml_broker
        self._connected = ml_broker is not None
    
    def publish(self, reading: Reading) -> bool:
        """Publish reading to ML broker."""
        if not self._ml_broker or not ML_AVAILABLE:
            return False
        
        try:
            ml_reading = MLReading(
                sensor_id=reading.sensor_id,
                sensor_type=reading.sensor_type,
                value=reading.value,
                timestamp=reading.timestamp,
            )
            self._ml_broker.publish(ml_reading)
            return True
        except Exception as e:
            logger.warning("ML broker publish failed: %s", e)
            return False
    
    def is_connected(self) -> bool:
        return self._connected and ML_AVAILABLE


def create_ml_broker_adapter() -> IReadingBroker:
    """Factory to create ML broker adapter.
    
    Returns NullBroker if ML is not available.
    """
    from ..core.domain.broker_interface import NullBroker
    
    if not ML_AVAILABLE:
        logger.info("ML not available, using NullBroker")
        return NullBroker()
    
    try:
        from iot_machine_learning.ml_service.broker import get_reading_broker
        ml_broker = get_reading_broker()
        return MLBrokerAdapter(ml_broker)
    except Exception as e:
        logger.warning("Failed to create ML broker: %s", e)
        return NullBroker()

"""Abstract interface for reading broker.

This decouples Ingesta from ML implementation details.
Any broker implementation (Redis, MQTT, in-memory) can implement this interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class Reading:
    """Reading data to publish to broker."""
    sensor_id: int
    sensor_type: str
    value: float
    timestamp: float
    device_id: Optional[int] = None
    device_uuid: Optional[str] = None
    sensor_uuid: Optional[str] = None
    sequence: Optional[int] = None


class IReadingBroker(ABC):
    """Abstract interface for reading broker.
    
    Implementations:
    - RedisReadingBroker: Publishes to Redis Streams
    - MQTTReadingBroker: Publishes to MQTT
    - NullBroker: No-op for testing
    """
    
    @abstractmethod
    def publish(self, reading: Reading) -> bool:
        """Publish a reading to the broker.
        
        Args:
            reading: Reading data to publish
            
        Returns:
            True if published successfully, False otherwise
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """Check if broker is connected."""
        pass


class NullBroker(IReadingBroker):
    """No-op broker for testing or when ML is disabled."""
    
    def publish(self, reading: Reading) -> bool:
        return True
    
    def is_connected(self) -> bool:
        return True

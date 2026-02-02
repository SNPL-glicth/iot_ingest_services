"""Statistics for MQTT receiver.

Extracted from receiver.py for modularity.
"""

from __future__ import annotations


class ReceiverStats:
    """Estadísticas del receptor MQTT."""
    
    def __init__(self):
        self.received = 0
        self.processed = 0
        self.failed = 0
        self.duplicates = 0
        self.last_message_at: float = 0
    
    def __str__(self) -> str:
        return (
            f"Stats: received={self.received} processed={self.processed} "
            f"failed={self.failed} duplicates={self.duplicates}"
        )
    
    def to_dict(self) -> dict:
        """Convert stats to dictionary."""
        return {
            "received": self.received,
            "processed": self.processed,
            "failed": self.failed,
            "duplicates": self.duplicates,
            "last_message_at": self.last_message_at,
        }

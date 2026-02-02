"""Estadísticas de procesamiento."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class Stats:
    """Estadísticas de procesamiento de mensajes."""
    
    received: int = 0
    processed: int = 0
    failed: int = 0
    last_message_at: float = 0
    started_at: datetime = field(default_factory=datetime.utcnow)
    
    def __str__(self) -> str:
        return f"Stats: received={self.received} processed={self.processed} failed={self.failed}"
    
    def to_dict(self) -> dict:
        """Convierte a diccionario."""
        return {
            "received": self.received,
            "processed": self.processed,
            "failed": self.failed,
            "last_message_at": self.last_message_at,
            "started_at": self.started_at.isoformat(),
            "success_rate": self._success_rate(),
        }
    
    def _success_rate(self) -> float:
        """Calcula tasa de éxito."""
        total = self.processed + self.failed
        if total == 0:
            return 1.0
        return self.processed / total
    
    def reset(self):
        """Reinicia estadísticas."""
        self.received = 0
        self.processed = 0
        self.failed = 0
        self.last_message_at = 0
        self.started_at = datetime.utcnow()

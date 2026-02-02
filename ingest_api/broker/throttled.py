"""Broker con throttling para limitar frecuencia de publicación.

Envuelve el broker de ML y limita la frecuencia de publicación
por sensor para evitar saturar a los consumidores.
"""

from __future__ import annotations

from typing import Dict

from ..core.domain.broker_interface import IReadingBroker, Reading


class ThrottledReadingBroker(IReadingBroker):
    """Broker con throttling por sensor.
    
    Limita la frecuencia de publicación para evitar saturar
    a los consumidores (ML Worker, Decision Orchestrator).
    """
    
    def __init__(
        self, 
        inner: IReadingBroker, 
        *, 
        min_interval_seconds: float = 1.0
    ) -> None:
        self._inner = inner
        self._min_interval_seconds = float(min_interval_seconds)
        self._last_published_ts_by_sensor: Dict[int, float] = {}
    
    def publish(self, reading: Reading) -> None:
        """Publica si ha pasado el intervalo mínimo desde la última publicación."""
        last_ts = self._last_published_ts_by_sensor.get(reading.sensor_id)
        
        if last_ts is not None:
            elapsed = reading.timestamp - last_ts
            if elapsed < self._min_interval_seconds:
                return
        
        self._last_published_ts_by_sensor[reading.sensor_id] = float(reading.timestamp)
        self._inner.publish(reading)
    
    def is_connected(self) -> bool:
        """Check if broker is connected."""
        return self._inner.is_connected()

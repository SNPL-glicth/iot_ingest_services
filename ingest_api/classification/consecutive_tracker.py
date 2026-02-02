"""Tracker de lecturas consecutivas.

Rastrea lecturas consecutivas fuera de umbral para
determinar cuándo generar alertas.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict


@dataclass
class ConsecutiveState:
    """Estado de lecturas consecutivas de un sensor."""
    count: int = 0
    last_state: str = 'NORMAL'
    last_value: float = None


class ConsecutiveTracker:
    """Rastrea lecturas consecutivas en el mismo estado.
    
    Reglas:
    - Si el estado cambia (ej: NORMAL → ALERT), resetea contador a 1
    - Si el estado se mantiene (ej: ALERT → ALERT), incrementa contador
    - Si vuelve a NORMAL, resetea contador a 0
    """
    
    DEFAULT_CONSECUTIVE_READINGS = 3
    
    def __init__(self):
        self._cache: Dict[int, ConsecutiveState] = {}
    
    def update(self, sensor_id: int, new_state: str, value: float) -> int:
        """Actualiza y retorna el conteo de lecturas consecutivas.
        
        Args:
            sensor_id: ID del sensor
            new_state: Nuevo estado ('NORMAL', 'WARNING', 'ALERT')
            value: Valor actual de la lectura
            
        Returns:
            Número de lecturas consecutivas en el estado actual
        """
        current = self._cache.get(sensor_id, ConsecutiveState())
        
        if new_state == 'NORMAL':
            self._cache[sensor_id] = ConsecutiveState(
                count=0, last_state='NORMAL', last_value=value
            )
            return 0
        
        if current.last_state == new_state:
            new_count = current.count + 1
        else:
            new_count = 1
        
        self._cache[sensor_id] = ConsecutiveState(
            count=new_count, last_state=new_state, last_value=value
        )
        
        return new_count
    
    def get_count(self, sensor_id: int) -> int:
        """Obtiene el conteo actual de lecturas consecutivas."""
        state = self._cache.get(sensor_id)
        return state.count if state else 0
    
    def reset(self, sensor_id: int):
        """Resetea el tracker para un sensor."""
        self._cache.pop(sensor_id, None)

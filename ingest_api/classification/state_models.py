"""Modelos de estado operacional del sensor."""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


class SensorOperationalState(Enum):
    """Estados operacionales del sensor."""
    
    INITIALIZING = "INITIALIZING"  # Warm-up, no puede generar eventos
    NORMAL = "NORMAL"              # Operando, puede generar WARNING/ALERT
    WARNING = "WARNING"            # Delta spike activo
    ALERT = "ALERT"                # Violación de umbral activa
    STALE = "STALE"                # Sin lecturas recientes
    UNKNOWN = "UNKNOWN"            # Estado no determinable


@dataclass
class SensorStateInfo:
    """Información del estado actual del sensor."""
    
    sensor_id: int
    state: SensorOperationalState
    valid_readings_count: int
    min_readings_for_normal: int
    state_changed_at: Optional[datetime]
    can_generate_events: bool
    
    @property
    def is_warming_up(self) -> bool:
        """True si el sensor está en warm-up (INITIALIZING)."""
        return self.state == SensorOperationalState.INITIALIZING
    
    @property
    def readings_until_normal(self) -> int:
        """Lecturas restantes para transicionar a NORMAL."""
        if self.state != SensorOperationalState.INITIALIZING:
            return 0
        return max(0, self.min_readings_for_normal - self.valid_readings_count)


# Transiciones válidas de la máquina de estados
VALID_TRANSITIONS = {
    SensorOperationalState.INITIALIZING: {
        SensorOperationalState.NORMAL,
        SensorOperationalState.STALE,
    },
    SensorOperationalState.NORMAL: {
        SensorOperationalState.WARNING,
        SensorOperationalState.ALERT,
        SensorOperationalState.STALE,
    },
    SensorOperationalState.WARNING: {
        SensorOperationalState.NORMAL,
        SensorOperationalState.ALERT,
        SensorOperationalState.STALE,
    },
    SensorOperationalState.ALERT: {
        SensorOperationalState.NORMAL,
        SensorOperationalState.STALE,
    },
    SensorOperationalState.STALE: {
        SensorOperationalState.INITIALIZING,
    },
}


def is_valid_transition(from_state: SensorOperationalState, to_state: SensorOperationalState) -> bool:
    """Verifica si una transición de estado es válida."""
    if from_state == to_state:
        return True
    allowed = VALID_TRANSITIONS.get(from_state, set())
    return to_state in allowed

"""Gestor de estado operacional del sensor.

FUENTE ÚNICA DE VERDAD para el estado del sensor.
"""

from __future__ import annotations

from typing import Optional, Tuple

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from .state_models import (
    SensorOperationalState, 
    SensorStateInfo, 
    is_valid_transition,
)
from .state_repository import StateRepository


class SensorStateManager:
    """Gestor de estado operacional del sensor.
    
    ÚNICO PUNTO DE DECISIÓN para:
    - Consultar si un sensor puede generar eventos
    - Transicionar estados
    - Registrar lecturas válidas
    """
    
    DEFAULT_MIN_READINGS = 10
    
    def __init__(self, db: Session | Connection) -> None:
        self._db = db
        self._repo = StateRepository(db)
        self._cache: dict[int, SensorStateInfo] = {}
    
    def get_state(self, sensor_id: int) -> SensorStateInfo:
        """Obtiene el estado actual del sensor."""
        if sensor_id in self._cache:
            return self._cache[sensor_id]
        
        if self._repo.check_columns_exist():
            info = self._repo.get_state_from_db(sensor_id)
        else:
            info = self._repo.get_state_fallback(sensor_id)
        
        self._cache[sensor_id] = info
        return info
    
    def can_generate_events(self, sensor_id: int) -> Tuple[bool, str]:
        """Verifica si el sensor puede generar WARNING/ALERT.
        
        Returns:
            (can_generate, reason)
        """
        info = self.get_state(sensor_id)
        
        if info.state == SensorOperationalState.UNKNOWN:
            return False, "Sensor no encontrado"
        
        if info.state == SensorOperationalState.INITIALIZING:
            return False, f"Warm-up ({info.valid_readings_count}/{info.min_readings_for_normal})"
        
        if info.state == SensorOperationalState.STALE:
            return False, "Sensor inactivo (STALE)"
        
        return True, f"Estado {info.state.value}"
    
    def register_valid_reading(self, sensor_id: int) -> SensorStateInfo:
        """Registra una lectura válida y actualiza estado si aplica."""
        self._cache.pop(sensor_id, None)
        
        if self._repo.check_columns_exist():
            self._repo.increment_valid_readings(sensor_id)
            return self._repo.get_state_from_db(sensor_id)
        else:
            return self._repo.get_state_fallback(sensor_id)
    
    def transition_to(
        self, 
        sensor_id: int, 
        new_state: SensorOperationalState,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """Transiciona el sensor a un nuevo estado."""
        if not self._repo.check_columns_exist():
            return False, "Columnas de estado no existen"
        
        self._cache.pop(sensor_id, None)
        current = self.get_state(sensor_id)
        
        if not is_valid_transition(current.state, new_state):
            return False, f"Transición inválida: {current.state.value} → {new_state.value}"
        
        reset_count = new_state == SensorOperationalState.INITIALIZING
        rows = self._repo.update_state(sensor_id, new_state, current.state, reset_count)
        
        if rows == 0:
            self._cache.pop(sensor_id, None)
            actual = self.get_state(sensor_id)
            return False, f"Race condition: {current.state.value} → {actual.state.value}"
        
        self._cache.pop(sensor_id, None)
        return True, f"{current.state.value} → {new_state.value}"
    
    def clear_cache(self, sensor_id: Optional[int] = None) -> None:
        """Limpia el cache de estados."""
        if sensor_id:
            self._cache.pop(sensor_id, None)
        else:
            self._cache.clear()
    
    def on_threshold_violated(
        self, 
        sensor_id: int, 
        severity: str,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str, bool]:
        """Maneja transición cuando se viola un umbral.
        
        Returns:
            (success, message, is_new_transition)
        """
        current = self.get_state(sensor_id)
        sev_lower = (severity or "warning").lower()
        
        target = SensorOperationalState.ALERT if sev_lower == "critical" else SensorOperationalState.WARNING
        
        if current.state == target:
            return True, f"Ya en {target.value}", False
        
        if current.state == SensorOperationalState.ALERT and target == SensorOperationalState.WARNING:
            return True, "Mantener ALERT", False
        
        if current.state not in (
            SensorOperationalState.NORMAL,
            SensorOperationalState.WARNING,
            SensorOperationalState.ALERT,
        ):
            return False, f"Estado {current.state.value} no genera eventos", False
        
        success, msg = self.transition_to(sensor_id, target, reason)
        return success, msg, success
    
    def on_value_back_to_normal(
        self, 
        sensor_id: int,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str, bool]:
        """Maneja transición cuando el valor vuelve a rango normal.
        
        Returns:
            (success, message, was_in_alert_state)
        """
        current = self.get_state(sensor_id)
        
        if current.state == SensorOperationalState.NORMAL:
            return True, "Ya en NORMAL", False
        
        if current.state in (SensorOperationalState.WARNING, SensorOperationalState.ALERT):
            success, msg = self.transition_to(sensor_id, SensorOperationalState.NORMAL, reason)
            return success, msg, True
        
        return False, f"Estado {current.state.value} no aplica", False
    
    def sync_state_with_events(self, sensor_id: int) -> Tuple[bool, str]:
        """Sincroniza el estado del sensor con los eventos ML activos."""
        current = self.get_state(sensor_id)
        active_count = self._repo.get_active_event_count(sensor_id)
        
        if active_count > 0 and current.state == SensorOperationalState.NORMAL:
            self.transition_to(sensor_id, SensorOperationalState.WARNING, "sync")
            return True, "Sincronizado: NORMAL -> WARNING"
        
        if active_count == 0 and current.state in (
            SensorOperationalState.WARNING, SensorOperationalState.ALERT
        ):
            self.transition_to(sensor_id, SensorOperationalState.NORMAL, "sync")
            return True, f"Sincronizado: {current.state.value} -> NORMAL"
        
        return True, "Estado consistente"

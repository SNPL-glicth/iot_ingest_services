"""Módulo de Estado Operacional del Sensor.

FUENTE ÚNICA DE VERDAD para el estado del sensor.

Este módulo implementa la máquina de estados del sensor:
- INITIALIZING: Sensor recién creado o sin suficientes lecturas válidas
- NORMAL: Sensor operando normalmente, puede generar WARNING/ALERT
- WARNING: Sensor en estado de advertencia (delta spike activo)
- ALERT: Sensor en estado de alerta (violación de umbral activa)
- STALE: Sensor sin lecturas recientes

REGLA DE DOMINIO CRÍTICA:
Solo un sensor en estado NORMAL puede transicionar a WARNING o ALERT.
Un sensor en INITIALIZING NUNCA puede generar eventos.
"""

from __future__ import annotations

from enum import Enum
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Connection


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


class SensorStateManager:
    """Gestor de estado operacional del sensor.
    
    ÚNICO PUNTO DE DECISIÓN para:
    - Consultar si un sensor puede generar eventos
    - Transicionar estados
    - Registrar lecturas válidas
    """
    
    # Fallback si la columna no existe en BD (migración pendiente)
    # FIX: Aumentado a 10 para warm-up adecuado (alineado con SP)
    DEFAULT_MIN_READINGS = 10
    
    def __init__(self, db: Session | Connection) -> None:
        self._db = db
        self._cache: dict[int, SensorStateInfo] = {}
        self._columns_exist: Optional[bool] = None
    
    def _check_columns_exist(self) -> bool:
        """Verifica si las columnas de estado existen en la BD."""
        if self._columns_exist is not None:
            return self._columns_exist
        
        try:
            row = self._db.execute(
                text("""
                    SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS 
                    WHERE TABLE_SCHEMA = 'dbo' 
                    AND TABLE_NAME = 'sensors' 
                    AND COLUMN_NAME = 'operational_state'
                """)
            ).fetchone()
            self._columns_exist = row is not None
        except Exception:
            self._columns_exist = False
        
        return self._columns_exist
    
    def get_state(self, sensor_id: int) -> SensorStateInfo:
        """Obtiene el estado actual del sensor.
        
        Si las columnas de estado no existen, usa lógica de fallback
        basada en conteo de lecturas recientes.
        """
        if sensor_id in self._cache:
            return self._cache[sensor_id]
        
        if self._check_columns_exist():
            return self._get_state_from_db(sensor_id)
        else:
            return self._get_state_fallback(sensor_id)
    
    def _get_state_from_db(self, sensor_id: int) -> SensorStateInfo:
        """Obtiene estado desde columnas de BD."""
        row = self._db.execute(
            text("""
                SELECT 
                    operational_state,
                    valid_readings_count,
                    min_readings_for_normal,
                    state_changed_at
                FROM dbo.sensors
                WHERE id = :sensor_id
            """),
            {"sensor_id": sensor_id},
        ).fetchone()
        
        if not row:
            info = SensorStateInfo(
                sensor_id=sensor_id,
                state=SensorOperationalState.UNKNOWN,
                valid_readings_count=0,
                min_readings_for_normal=self.DEFAULT_MIN_READINGS,
                state_changed_at=None,
                can_generate_events=False,
            )
            self._cache[sensor_id] = info
            return info
        
        state_str = str(row.operational_state or "INITIALIZING").upper()
        try:
            state = SensorOperationalState(state_str)
        except ValueError:
            state = SensorOperationalState.UNKNOWN
        
        can_generate = state in (
            SensorOperationalState.NORMAL,
            SensorOperationalState.WARNING,
            SensorOperationalState.ALERT,
        )
        
        info = SensorStateInfo(
            sensor_id=sensor_id,
            state=state,
            valid_readings_count=int(row.valid_readings_count or 0),
            min_readings_for_normal=int(row.min_readings_for_normal or self.DEFAULT_MIN_READINGS),
            state_changed_at=row.state_changed_at,
            can_generate_events=can_generate,
        )
        self._cache[sensor_id] = info
        return info
    
    def _get_state_fallback(self, sensor_id: int) -> SensorStateInfo:
        """Fallback: calcula estado basado en lecturas recientes.
        
        Usado cuando la migración de BD no se ha aplicado.
        """
        row = self._db.execute(
            text("""
                SELECT COUNT(*) as cnt
                FROM dbo.sensor_readings
                WHERE sensor_id = :sensor_id
                AND timestamp >= DATEADD(HOUR, -2, GETDATE())
            """),
            {"sensor_id": sensor_id},
        ).fetchone()
        
        count = int(row.cnt) if row and row.cnt else 0
        
        if count >= self.DEFAULT_MIN_READINGS:
            state = SensorOperationalState.NORMAL
            can_generate = True
        else:
            state = SensorOperationalState.INITIALIZING
            can_generate = False
        
        info = SensorStateInfo(
            sensor_id=sensor_id,
            state=state,
            valid_readings_count=count,
            min_readings_for_normal=self.DEFAULT_MIN_READINGS,
            state_changed_at=None,
            can_generate_events=can_generate,
        )
        self._cache[sensor_id] = info
        return info
    
    def can_generate_events(self, sensor_id: int) -> Tuple[bool, str]:
        """Verifica si el sensor puede generar WARNING/ALERT.
        
        REGLA DE DOMINIO:
        Solo sensores en NORMAL, WARNING o ALERT pueden generar eventos.
        Sensores en INITIALIZING o STALE NO pueden.
        
        Returns:
            (can_generate, reason)
        """
        info = self.get_state(sensor_id)
        
        if info.state == SensorOperationalState.UNKNOWN:
            return False, "Sensor no encontrado"
        
        if info.state == SensorOperationalState.INITIALIZING:
            return False, f"Sensor en warm-up ({info.valid_readings_count}/{info.min_readings_for_normal} lecturas)"
        
        if info.state == SensorOperationalState.STALE:
            return False, "Sensor inactivo (STALE), requiere warm-up"
        
        return True, f"Sensor en estado {info.state.value}"
    
    def register_valid_reading(self, sensor_id: int) -> SensorStateInfo:
        """Registra una lectura válida y actualiza estado si aplica.
        
        Si el sensor está en INITIALIZING y alcanza el mínimo de lecturas,
        transiciona automáticamente a NORMAL.
        
        Si el sensor está en STALE, transiciona a INITIALIZING.
        """
        # Invalidar cache
        self._cache.pop(sensor_id, None)
        
        if self._check_columns_exist():
            return self._register_reading_db(sensor_id)
        else:
            return self._register_reading_fallback(sensor_id)
    
    def _register_reading_db(self, sensor_id: int) -> SensorStateInfo:
        """Registra lectura usando stored procedure."""
        try:
            # Usar SP si existe
            result = self._db.execute(
                text("""
                    DECLARE @can_generate BIT, @current_state VARCHAR(20);
                    EXEC dbo.sp_sensor_increment_valid_readings 
                        @sensor_id = :sensor_id,
                        @out_can_generate_events = @can_generate OUTPUT,
                        @out_current_state = @current_state OUTPUT;
                    SELECT @can_generate AS can_generate, @current_state AS current_state;
                """),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            if result:
                # Refrescar estado desde BD
                return self._get_state_from_db(sensor_id)
        except Exception:
            # SP no existe, usar UPDATE directo
            pass
        
        # Fallback: UPDATE directo
        self._db.execute(
            text("""
                UPDATE dbo.sensors
                SET valid_readings_count = valid_readings_count + 1
                WHERE id = :sensor_id
                AND operational_state = 'INITIALIZING'
            """),
            {"sensor_id": sensor_id},
        )
        
        # Verificar si debe transicionar a NORMAL
        self._db.execute(
            text("""
                UPDATE dbo.sensors
                SET 
                    operational_state = 'NORMAL',
                    state_changed_at = GETDATE()
                WHERE id = :sensor_id
                AND operational_state = 'INITIALIZING'
                AND valid_readings_count >= min_readings_for_normal
            """),
            {"sensor_id": sensor_id},
        )
        
        return self._get_state_from_db(sensor_id)
    
    def _register_reading_fallback(self, sensor_id: int) -> SensorStateInfo:
        """Fallback: recalcula estado basado en lecturas."""
        return self._get_state_fallback(sensor_id)
    
    def transition_to(
        self, 
        sensor_id: int, 
        new_state: SensorOperationalState,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """Transiciona el sensor a un nuevo estado.
        
        Valida que la transición sea válida según la máquina de estados.
        
        SECURITY FIX: Atomic transition with optimistic locking to prevent race conditions.
        - Invalidate cache BEFORE operation
        - Use conditional UPDATE with current state check
        - Re-read from DB after update to confirm
        
        Returns:
            (success, message)
        """
        if not self._check_columns_exist():
            return False, "Columnas de estado no existen en BD"
        
        # SECURITY FIX: Invalidate cache BEFORE reading to ensure fresh read
        self._cache.pop(sensor_id, None)
        
        current = self.get_state(sensor_id)
        
        # Validar transición
        valid_transitions = {
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
        
        allowed = valid_transitions.get(current.state, set())
        if new_state not in allowed and new_state != current.state:
            return False, f"Transición inválida: {current.state.value} → {new_state.value}"
        
        # Ejecutar transición con OPTIMISTIC LOCKING
        # SECURITY FIX: Only update if current state matches expected (prevents race condition)
        reset_count = 1 if new_state == SensorOperationalState.INITIALIZING else 0
        
        result = self._db.execute(
            text("""
                UPDATE dbo.sensors
                SET 
                    operational_state = :new_state,
                    state_changed_at = GETDATE(),
                    valid_readings_count = CASE 
                        WHEN :reset = 1 THEN 0 
                        ELSE valid_readings_count 
                    END
                WHERE id = :sensor_id
                  AND operational_state = :expected_state
            """),
            {
                "sensor_id": sensor_id,
                "new_state": new_state.value,
                "reset": reset_count,
                "expected_state": current.state.value,
            },
        )
        
        # SECURITY FIX: Check if update actually happened (rowcount > 0)
        rows_affected = result.rowcount if hasattr(result, 'rowcount') else 1
        
        if rows_affected == 0:
            # Race condition detected: state changed between read and write
            self._cache.pop(sensor_id, None)  # Ensure cache is cleared
            actual = self.get_state(sensor_id)
            return False, f"Race condition: estado cambió de {current.state.value} a {actual.state.value} durante transición"
        
        return True, f"Transición exitosa: {current.state.value} → {new_state.value}"
    
    def clear_cache(self, sensor_id: Optional[int] = None) -> None:
        """Limpia el cache de estados."""
        if sensor_id:
            self._cache.pop(sensor_id, None)
        else:
            self._cache.clear()
    
    # =========================================================================
    # MÉTODOS DE TRANSICIÓN BASADOS EN EVENTOS ML
    # Estos métodos implementan la máquina de estados REAL para alertas
    # =========================================================================
    
    def on_threshold_violated(
        self, 
        sensor_id: int, 
        severity: str,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str, bool]:
        """Maneja transición cuando se viola un umbral.
        
        REGLA DE DOMINIO:
        - NORMAL -> WARNING (si severity=warning)
        - NORMAL -> ALERT (si severity=critical)
        - WARNING -> ALERT (si severity=critical)
        - WARNING -> WARNING (solo update, NO crear nuevo evento)
        - ALERT -> ALERT (solo update, NO crear nuevo evento)
        
        Returns:
            (success, message, is_new_transition)
            is_new_transition=True significa que debe crearse evento
            is_new_transition=False significa que solo debe actualizarse
        """
        current = self.get_state(sensor_id)
        sev_lower = severity.lower() if severity else "warning"
        
        # Determinar estado destino
        if sev_lower == "critical":
            target_state = SensorOperationalState.ALERT
        else:
            target_state = SensorOperationalState.WARNING
        
        # Si ya está en el estado destino o superior, NO es nueva transición
        if current.state == target_state:
            return True, f"Sensor ya en {target_state.value}, solo update", False
        
        if current.state == SensorOperationalState.ALERT and target_state == SensorOperationalState.WARNING:
            # ALERT es más severo que WARNING, mantener ALERT
            return True, "Sensor en ALERT, mantener estado", False
        
        # Verificar si puede transicionar
        if current.state not in (
            SensorOperationalState.NORMAL,
            SensorOperationalState.WARNING,
            SensorOperationalState.ALERT,
        ):
            return False, f"Sensor en {current.state.value}, no puede generar eventos", False
        
        # Ejecutar transición
        success, msg = self.transition_to(sensor_id, target_state, reason)
        return success, msg, success  # is_new_transition = success
    
    def on_value_back_to_normal(
        self, 
        sensor_id: int,
        reason: Optional[str] = None,
    ) -> Tuple[bool, str, bool]:
        """Maneja transición cuando el valor vuelve a rango normal.
        
        REGLA DE DOMINIO:
        - WARNING -> NORMAL (resolver evento)
        - ALERT -> NORMAL (resolver evento)
        - NORMAL -> NORMAL (no-op)
        
        Returns:
            (success, message, was_in_alert_state)
            was_in_alert_state=True significa que había evento activo que resolver
        """
        current = self.get_state(sensor_id)
        
        # Si ya está en NORMAL, no hay nada que hacer
        if current.state == SensorOperationalState.NORMAL:
            return True, "Sensor ya en NORMAL", False
        
        # Si está en WARNING o ALERT, transicionar a NORMAL
        if current.state in (SensorOperationalState.WARNING, SensorOperationalState.ALERT):
            was_in_alert = current.state in (SensorOperationalState.WARNING, SensorOperationalState.ALERT)
            success, msg = self.transition_to(sensor_id, SensorOperationalState.NORMAL, reason)
            return success, msg, was_in_alert
        
        # Otros estados (INITIALIZING, STALE) no aplican
        return False, f"Sensor en {current.state.value}, transición a NORMAL no aplica", False
    
    def get_active_event_count(self, sensor_id: int) -> int:
        """Cuenta eventos activos para un sensor.
        
        Útil para verificar consistencia entre estado del sensor y eventos ML.
        """
        try:
            row = self._db.execute(
                text("""
                    SELECT COUNT(*) as cnt
                    FROM dbo.ml_events
                    WHERE sensor_id = :sensor_id
                    AND status = 'active'
                """),
                {"sensor_id": sensor_id},
            ).fetchone()
            return int(row.cnt) if row and row.cnt else 0
        except Exception:
            return 0
    
    def sync_state_with_events(self, sensor_id: int) -> Tuple[bool, str]:
        """Sincroniza el estado del sensor con los eventos ML activos.
        
        REGLA DE DOMINIO:
        - Si hay eventos activos pero sensor en NORMAL -> transicionar a WARNING/ALERT
        - Si no hay eventos activos pero sensor en WARNING/ALERT -> transicionar a NORMAL
        
        Esto garantiza consistencia entre la tabla sensors y ml_events.
        """
        current = self.get_state(sensor_id)
        active_count = self.get_active_event_count(sensor_id)
        
        if active_count > 0 and current.state == SensorOperationalState.NORMAL:
            # Hay eventos activos pero sensor en NORMAL -> inconsistencia
            # Verificar severidad del evento más severo
            row = self._db.execute(
                text("""
                    SELECT TOP 1 event_type
                    FROM dbo.ml_events
                    WHERE sensor_id = :sensor_id AND status = 'active'
                    ORDER BY 
                        CASE event_type 
                            WHEN 'critical' THEN 1 
                            WHEN 'warning' THEN 2 
                            ELSE 3 
                        END
                """),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            if row:
                event_type = str(row[0]).lower()
                if event_type == "critical":
                    self.transition_to(sensor_id, SensorOperationalState.ALERT, "sync_with_events")
                else:
                    self.transition_to(sensor_id, SensorOperationalState.WARNING, "sync_with_events")
                return True, f"Sensor sincronizado: NORMAL -> {event_type.upper()}"
        
        elif active_count == 0 and current.state in (SensorOperationalState.WARNING, SensorOperationalState.ALERT):
            # No hay eventos activos pero sensor en WARNING/ALERT -> inconsistencia
            self.transition_to(sensor_id, SensorOperationalState.NORMAL, "sync_with_events")
            return True, f"Sensor sincronizado: {current.state.value} -> NORMAL"
        
        return True, "Estado consistente"

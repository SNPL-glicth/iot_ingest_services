"""Repositorio de estado del sensor - acceso a BD."""

from __future__ import annotations

from typing import Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from .state_models import SensorOperationalState, SensorStateInfo


DEFAULT_MIN_READINGS = 10


class StateRepository:
    """Acceso a BD para estado de sensores."""
    
    def __init__(self, db: Session | Connection):
        self._db = db
        self._columns_exist: Optional[bool] = None
    
    def check_columns_exist(self) -> bool:
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
    
    def get_state_from_db(self, sensor_id: int) -> SensorStateInfo:
        """Obtiene estado desde columnas de BD."""
        row = self._db.execute(
            text("""
                SELECT operational_state, valid_readings_count,
                       min_readings_for_normal, state_changed_at
                FROM dbo.sensors WHERE id = :sensor_id
            """),
            {"sensor_id": sensor_id},
        ).fetchone()
        
        if not row:
            return SensorStateInfo(
                sensor_id=sensor_id,
                state=SensorOperationalState.UNKNOWN,
                valid_readings_count=0,
                min_readings_for_normal=DEFAULT_MIN_READINGS,
                state_changed_at=None,
                can_generate_events=False,
            )
        
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
        
        return SensorStateInfo(
            sensor_id=sensor_id,
            state=state,
            valid_readings_count=int(row.valid_readings_count or 0),
            min_readings_for_normal=int(row.min_readings_for_normal or DEFAULT_MIN_READINGS),
            state_changed_at=row.state_changed_at,
            can_generate_events=can_generate,
        )
    
    def get_state_fallback(self, sensor_id: int) -> SensorStateInfo:
        """Fallback: calcula estado basado en lecturas recientes."""
        row = self._db.execute(
            text("""
                SELECT COUNT(*) as cnt FROM dbo.sensor_readings
                WHERE sensor_id = :sensor_id
                AND timestamp >= DATEADD(HOUR, -2, GETDATE())
            """),
            {"sensor_id": sensor_id},
        ).fetchone()
        
        count = int(row.cnt) if row and row.cnt else 0
        
        if count >= DEFAULT_MIN_READINGS:
            state = SensorOperationalState.NORMAL
            can_generate = True
        else:
            state = SensorOperationalState.INITIALIZING
            can_generate = False
        
        return SensorStateInfo(
            sensor_id=sensor_id,
            state=state,
            valid_readings_count=count,
            min_readings_for_normal=DEFAULT_MIN_READINGS,
            state_changed_at=None,
            can_generate_events=can_generate,
        )
    
    def increment_valid_readings(self, sensor_id: int) -> bool:
        """Incrementa contador de lecturas válidas."""
        try:
            self._db.execute(
                text("""
                    UPDATE dbo.sensors
                    SET valid_readings_count = valid_readings_count + 1
                    WHERE id = :sensor_id AND operational_state = 'INITIALIZING'
                """),
                {"sensor_id": sensor_id},
            )
            
            self._db.execute(
                text("""
                    UPDATE dbo.sensors
                    SET operational_state = 'NORMAL', state_changed_at = GETDATE()
                    WHERE id = :sensor_id
                    AND operational_state = 'INITIALIZING'
                    AND valid_readings_count >= min_readings_for_normal
                """),
                {"sensor_id": sensor_id},
            )
            return True
        except Exception:
            return False
    
    def update_state(
        self,
        sensor_id: int,
        new_state: SensorOperationalState,
        expected_state: SensorOperationalState,
        reset_count: bool = False,
    ) -> int:
        """Actualiza estado con optimistic locking. Retorna rows affected."""
        result = self._db.execute(
            text("""
                UPDATE dbo.sensors
                SET operational_state = :new_state,
                    state_changed_at = GETDATE(),
                    valid_readings_count = CASE WHEN :reset = 1 THEN 0 
                                           ELSE valid_readings_count END
                WHERE id = :sensor_id AND operational_state = :expected_state
            """),
            {
                "sensor_id": sensor_id,
                "new_state": new_state.value,
                "reset": 1 if reset_count else 0,
                "expected_state": expected_state.value,
            },
        )
        return result.rowcount if hasattr(result, 'rowcount') else 1
    
    def get_active_event_count(self, sensor_id: int) -> int:
        """Cuenta eventos ML activos para un sensor."""
        try:
            row = self._db.execute(
                text("""
                    SELECT COUNT(*) as cnt FROM dbo.ml_events
                    WHERE sensor_id = :sensor_id AND status = 'active'
                """),
                {"sensor_id": sensor_id},
            ).fetchone()
            return int(row.cnt) if row and row.cnt else 0
        except Exception:
            return 0

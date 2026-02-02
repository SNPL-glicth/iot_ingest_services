"""Ejecutor del Stored Procedure de dominio."""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..domain.reading import Reading

logger = logging.getLogger(__name__)

SP_NAME = "sp_insert_reading_and_check_threshold"


class SPExecutor:
    """Ejecuta el SP de inserción y evaluación de umbrales.
    
    El SP maneja TODO el pipeline de dominio:
    - INSERT en sensor_readings
    - Evaluación de umbrales (warning/critical)
    - Creación de alerts
    - Creación de alert_notifications
    - Detección de delta spike
    - Creación de ml_events
    """
    
    def __init__(self, engine: Engine):
        self._engine = engine
    
    def execute(self, reading: Reading) -> bool:
        """Ejecuta el SP para una lectura.
        
        Args:
            reading: Lectura validada
            
        Returns:
            True si se ejecutó correctamente
        """
        try:
            params = reading.to_sp_params()
            
            with self._engine.begin() as conn:
                conn.execute(
                    text(f"""
                        EXEC {SP_NAME}
                            @p_sensor_id = :sensor_id, 
                            @p_value = :value,
                            @p_device_timestamp = :device_ts
                    """),
                    params,
                )
            
            logger.debug(
                "[SP] Executed: sensor_id=%d value=%.4f",
                reading.sensor_id,
                reading.value,
            )
            return True
            
        except Exception as e:
            logger.error(
                "[SP] Failed: sensor_id=%d error=%s",
                reading.sensor_id,
                e,
            )
            return False

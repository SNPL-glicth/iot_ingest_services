"""Detector de delta spikes.

Detecta cambios bruscos en lecturas de sensores usando
umbrales de delta y slope.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from .models import DeltaThreshold, LastReading
from .thresholds import ThresholdManager


# Umbrales de ruido por tipo de sensor
SENSOR_TYPE_NOISE_THRESHOLDS = {
    'temperature': (0.5, 0.02),    # 0.5°C abs, 2% rel
    'humidity': (2.0, 0.03),        # 2% abs, 3% rel
    'air_quality': (50.0, 0.10),    # 50 ppm abs, 10% rel
    'voltage': (1.0, 0.05),         # 1V abs, 5% rel
    'power': (10.0, 0.10),          # 10W abs, 10% rel
    'pressure': (0.5, 0.005),       # 0.5 hPa abs, 0.5% rel
    'default': (0.1, 0.01),         # Conservador
}


class DeltaDetector:
    """Detecta delta spikes en lecturas de sensores."""
    
    COOLDOWN_SECONDS = 300  # 5 minutos entre eventos
    
    def __init__(self, db: Session | Connection, threshold_manager: ThresholdManager):
        self._db = db
        self._thresholds = threshold_manager
        self._sensor_type_cache: dict[int, str] = {}
        self._cooldown_cache: dict[int, dict[str, datetime]] = {}
    
    def check_delta_spike(
        self,
        sensor_id: int,
        current_value: float,
        current_ts: datetime,
        last_reading: LastReading,
    ) -> Optional[dict]:
        """Verifica si hay un delta spike.
        
        Returns:
            dict con is_spike=True si se detecta spike, None en caso contrario
        """
        delta_threshold = self._thresholds.get_delta_threshold(sensor_id)
        if not delta_threshold:
            return None

        # Obtener umbrales de ruido específicos
        sensor_type = self._get_sensor_type(sensor_id)
        noise_abs, noise_rel = SENSOR_TYPE_NOISE_THRESHOLDS.get(
            sensor_type, SENSOR_TYPE_NOISE_THRESHOLDS['default']
        )

        # Calcular delta y dt
        delta_abs = abs(current_value - last_reading.value)
        dt_seconds = (current_ts - last_reading.timestamp).total_seconds()

        # Calcular delta relativo
        delta_rel = 0.0
        if abs(last_reading.value) > 1e-6:
            delta_rel = abs(delta_abs / last_reading.value)

        # Filtrar ruido
        is_noise = delta_abs < noise_abs and delta_rel < noise_rel
        if is_noise:
            return None

        # Evitar división por cero
        if dt_seconds <= 0:
            dt_seconds = 0.001

        slope_abs = delta_abs / dt_seconds
        slope_rel = delta_rel / dt_seconds if dt_seconds > 0 else 0.0

        # Verificar umbrales
        triggered = []
        reason_parts = []

        if delta_threshold.abs_delta is not None and delta_abs >= delta_threshold.abs_delta:
            triggered.append("abs_delta")
            reason_parts.append(f"delta_abs={delta_abs:.4f} >= {delta_threshold.abs_delta:.4f}")

        if delta_threshold.rel_delta is not None and delta_rel >= delta_threshold.rel_delta:
            triggered.append("rel_delta")
            reason_parts.append(f"delta_rel={delta_rel:.4%} >= {delta_threshold.rel_delta:.4%}")

        if delta_threshold.abs_slope is not None and slope_abs >= delta_threshold.abs_slope:
            triggered.append("abs_slope")
            reason_parts.append(f"slope_abs={slope_abs:.4f} >= {delta_threshold.abs_slope:.4f}")

        if delta_threshold.rel_slope is not None and slope_rel >= delta_threshold.rel_slope:
            triggered.append("rel_slope")
            reason_parts.append(f"slope_rel={slope_rel:.4f} >= {delta_threshold.rel_slope:.4f}")

        if not triggered:
            return None

        return {
            "is_spike": True,
            "delta_abs": delta_abs,
            "delta_rel": delta_rel,
            "slope_abs": slope_abs,
            "slope_rel": slope_rel,
            "dt_seconds": dt_seconds,
            "last_value": last_reading.value,
            "triggered_thresholds": triggered,
            "severity": delta_threshold.severity,
            "reason": "; ".join(reason_parts),
        }

    def is_in_cooldown(self, sensor_id: int, event_type: str) -> bool:
        """Verifica si el sensor está en período de cooldown."""
        now = datetime.now(timezone.utc)
        
        if sensor_id not in self._cooldown_cache:
            self._cooldown_cache[sensor_id] = {}
        
        last_event = self._cooldown_cache[sensor_id].get(event_type)
        if last_event:
            elapsed = (now - last_event).total_seconds()
            if elapsed < self.COOLDOWN_SECONDS:
                return True
        
        self._cooldown_cache[sensor_id][event_type] = now
        return False

    def clear_cooldown(self, sensor_id: int, event_type: str = None):
        """Limpia el cooldown de un sensor."""
        if sensor_id in self._cooldown_cache:
            if event_type:
                self._cooldown_cache[sensor_id].pop(event_type, None)
            else:
                self._cooldown_cache[sensor_id] = {}

    def _get_sensor_type(self, sensor_id: int) -> str:
        """Obtiene el tipo de sensor desde la BD."""
        if sensor_id in self._sensor_type_cache:
            return self._sensor_type_cache[sensor_id]
        
        try:
            row = self._db.execute(
                text("SELECT sensor_type FROM dbo.sensors WHERE id = :sensor_id"),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            sensor_type = 'default'
            if row and row[0]:
                sensor_type = str(row[0]).lower().strip()
            
            self._sensor_type_cache[sensor_id] = sensor_type
            return sensor_type
        except Exception:
            return 'default'

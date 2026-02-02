"""Gestión de umbrales para clasificación de lecturas.

Maneja la obtención y cache de umbrales desde la BD:
- Umbrales canónicos (WARNING/ALERT)
- Rangos físicos
- Umbrales de delta/spike
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

# Import condicional para evitar dependencia cuando ML no está instalado
try:
    from iot_machine_learning.ml_service.utils.numeric_precision import safe_float
except ImportError:
    def safe_float(value, default=0.0):
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

from .models import CanonicalThresholds, PhysicalRange, DeltaThreshold, LastReading


class ThresholdManager:
    """Gestiona umbrales desde la BD con cache."""
    
    def __init__(self, db: Session | Connection):
        self._db = db
        self._range_cache: dict[int, Optional[PhysicalRange]] = {}
        self._delta_cache: dict[int, Optional[DeltaThreshold]] = {}
        self._last_reading_cache: dict[int, Optional[LastReading]] = {}
        self._thresholds_cache: dict[int, Optional[CanonicalThresholds]] = {}
    
    def get_canonical_thresholds(self, sensor_id: int) -> Optional[CanonicalThresholds]:
        """Obtiene umbrales WARNING/ALERT desde alert_thresholds."""
        if sensor_id in self._thresholds_cache:
            return self._thresholds_cache[sensor_id]

        rows = self._db.execute(
            text("""
                SELECT severity, threshold_value_min, threshold_value_max
                FROM dbo.alert_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                  AND condition_type = 'out_of_range'
                  AND severity IN ('warning', 'critical')
                ORDER BY CASE severity WHEN 'critical' THEN 0 ELSE 1 END, id ASC
            """),
            {"sensor_id": sensor_id},
        ).fetchall()

        if not rows:
            self._thresholds_cache[sensor_id] = None
            return None

        warning_min, warning_max = None, None
        alert_min, alert_max = None, None

        for r in rows:
            sev = str(getattr(r, "severity", "") or "").lower()
            min_v = safe_float(getattr(r, "threshold_value_min", None), None)
            max_v = safe_float(getattr(r, "threshold_value_max", None), None)
            if sev == "warning" and warning_min is None:
                warning_min, warning_max = min_v, max_v
            elif sev == "critical" and alert_min is None:
                alert_min, alert_max = min_v, max_v

        th = CanonicalThresholds(
            warning_min=warning_min, warning_max=warning_max,
            alert_min=alert_min, alert_max=alert_max,
        )
        self._thresholds_cache[sensor_id] = th
        return th

    def get_physical_range(self, sensor_id: int) -> Optional[PhysicalRange]:
        """Obtiene el rango físico del sensor."""
        if sensor_id in self._range_cache:
            return self._range_cache[sensor_id]

        row = self._db.execute(
            text("""
                SELECT TOP 1 id, threshold_value_min, threshold_value_max
                FROM dbo.alert_thresholds
                WHERE sensor_id = :sensor_id
                  AND is_active = 1
                  AND condition_type = 'out_of_range'
                ORDER BY id ASC
            """),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._range_cache[sensor_id] = None
            return None

        min_val = safe_float(row.threshold_value_min, None) if row.threshold_value_min is not None else None
        max_val = safe_float(row.threshold_value_max, None) if row.threshold_value_max is not None else None

        if min_val is None and max_val is None:
            self._range_cache[sensor_id] = None
            return None

        physical_range = PhysicalRange(
            min_value=min_val, max_value=max_val, threshold_id=int(row.id),
        )
        self._range_cache[sensor_id] = physical_range
        return physical_range

    def get_delta_threshold(self, sensor_id: int) -> Optional[DeltaThreshold]:
        """Obtiene los umbrales de delta para el sensor."""
        if sensor_id in self._delta_cache:
            return self._delta_cache[sensor_id]

        row = self._db.execute(
            text("""
                SELECT TOP 1 abs_delta, rel_delta, abs_slope, rel_slope, severity
                FROM dbo.delta_thresholds
                WHERE sensor_id = :sensor_id AND is_active = 1
                ORDER BY id ASC
            """),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._delta_cache[sensor_id] = None
            return None

        delta_threshold = DeltaThreshold(
            abs_delta=safe_float(row.abs_delta, None) if row.abs_delta is not None else None,
            rel_delta=safe_float(row.rel_delta, None) if row.rel_delta is not None else None,
            abs_slope=safe_float(row.abs_slope, None) if row.abs_slope is not None else None,
            rel_slope=safe_float(row.rel_slope, None) if row.rel_slope is not None else None,
            severity=str(row.severity or "warning"),
        )
        self._delta_cache[sensor_id] = delta_threshold
        return delta_threshold

    def get_last_reading(self, sensor_id: int) -> Optional[LastReading]:
        """Obtiene la última lectura del sensor."""
        if sensor_id in self._last_reading_cache:
            return self._last_reading_cache[sensor_id]

        row = self._db.execute(
            text("""
                SELECT TOP 1 latest_value, latest_timestamp
                FROM dbo.sensor_readings_latest
                WHERE sensor_id = :sensor_id
            """),
            {"sensor_id": sensor_id},
        ).fetchone()

        if not row:
            self._last_reading_cache[sensor_id] = None
            return None

        latest_val = safe_float(row.latest_value, None)
        if latest_val is None:
            self._last_reading_cache[sensor_id] = None
            return None
        
        last_reading = LastReading(value=latest_val, timestamp=row.latest_timestamp)
        self._last_reading_cache[sensor_id] = last_reading
        return last_reading

    def get_consecutive_readings_required(self, sensor_id: int, default: int = 3) -> int:
        """Obtiene lecturas consecutivas requeridas para alertar."""
        try:
            row = self._db.execute(
                text("""
                    SELECT TOP 1 consecutive_readings
                    FROM dbo.alert_thresholds
                    WHERE sensor_id = :sensor_id
                      AND is_active = 1
                      AND consecutive_readings IS NOT NULL
                    ORDER BY id ASC
                """),
                {"sensor_id": sensor_id},
            ).fetchone()
            
            if row and row[0]:
                return int(row[0])
        except Exception:
            pass
        
        return default

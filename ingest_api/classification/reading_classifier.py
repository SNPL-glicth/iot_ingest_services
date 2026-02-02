"""Clasificador principal de lecturas.

Clasifica lecturas en 3 categorías:
1. ALERT: Violación de rango físico
2. WARNING: Delta spike detectado
3. ML_PREDICTION: Dato limpio para ML
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session

from iot_machine_learning.ml_service.utils.numeric_precision import safe_float, is_valid_sensor_value

from .models import ReadingClass, ClassifiedReading, PhysicalRange
from .state_manager import SensorStateManager
from .state_models import SensorOperationalState
from .thresholds import ThresholdManager
from .delta_detector import DeltaDetector
from .consecutive_tracker import ConsecutiveTracker


class ReadingClassifier:
    """Clasificador de lecturas por propósito.
    
    MODELO DE ESTADOS:
    Usa SensorStateManager como único punto de decisión para determinar
    si un sensor puede generar WARNING/ALERT.
    
    Regla de dominio:
    - Sensor en INITIALIZING → NUNCA genera eventos
    - Sensor en STALE → NUNCA genera eventos
    - Sensor en NORMAL → puede generar WARNING/ALERT
    """

    def __init__(self, db: Session | Connection) -> None:
        self._db = db
        self._state_manager = SensorStateManager(db)
        self._thresholds = ThresholdManager(db)
        self._delta_detector = DeltaDetector(db, self._thresholds)
        self._consecutive = ConsecutiveTracker()

    def classify(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime] = None,
        ingest_timestamp: Optional[datetime] = None,
    ) -> ClassifiedReading:
        """Clasifica una lectura según su propósito.

        Orden de evaluación:
        0. MODELO DE ESTADOS: Verificar si sensor puede generar eventos
        1. Verificar violación de rango físico → ALERT
        2. Verificar delta spike → WARNING
        3. Resto → ML_PREDICTION
        """
        if ingest_timestamp is None:
            ingest_timestamp = datetime.now(timezone.utc)

        # Validación NaN/Infinity
        if not is_valid_sensor_value(value):
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=safe_float(value, 0.0),
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Valor inválido (NaN/Infinity/None): {value}",
            )

        # PASO 0: Verificar si sensor puede generar eventos
        self._state_manager.register_valid_reading(sensor_id)
        can_generate, state_reason = self._state_manager.can_generate_events(sensor_id)
        
        if not can_generate:
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Sensor bloqueado: {state_reason}",
            )

        # PASO 1: Verificar violación de rango físico
        result = self._check_physical_range(sensor_id, value, device_timestamp)
        if result:
            return result

        # Resetear contador si está dentro del rango
        self._consecutive.update(sensor_id, 'NORMAL', value)

        # Verificar si está dentro del rango WARNING (no evaluar delta spike)
        thresholds = self._thresholds.get_canonical_thresholds(sensor_id)
        if self._is_within_warning_band(value, thresholds):
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason="Dentro de rango WARNING; delta spike no aplica",
            )

        # PASO 2: Verificar delta spike
        result = self._check_delta_spike(sensor_id, value, device_timestamp, ingest_timestamp)
        if result:
            return result

        # Dato limpio para ML
        return ClassifiedReading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            classification=ReadingClass.ML_PREDICTION,
            reason="Dato dentro de rango físico y sin delta spike",
        )

    def _check_physical_range(
        self, sensor_id: int, value: float, device_timestamp: Optional[datetime]
    ) -> Optional[ClassifiedReading]:
        """Verifica violación de rango físico."""
        physical_range = self._thresholds.get_physical_range(sensor_id)
        if not physical_range:
            return None
        
        if not self._violates_range(value, physical_range):
            return None
        
        consecutive_required = self._thresholds.get_consecutive_readings_required(sensor_id)
        consecutive_count = self._consecutive.update(sensor_id, 'ALERT', value)
        
        if consecutive_count < consecutive_required:
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason=f"Fuera de rango, {consecutive_count}/{consecutive_required} consecutivas",
            )
        
        self._state_manager.transition_to(sensor_id, SensorOperationalState.ALERT)
        
        return ClassifiedReading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            classification=ReadingClass.ALERT,
            physical_range=physical_range,
            reason=f"Valor {value} fuera de rango [{physical_range.min_value}, {physical_range.max_value}]",
        )

    def _check_delta_spike(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime],
        ingest_timestamp: datetime,
    ) -> Optional[ClassifiedReading]:
        """Verifica delta spike."""
        last_reading = self._thresholds.get_last_reading(sensor_id)
        if not last_reading:
            return None
        
        # Validar que el historial sea reciente (máximo 10 minutos)
        last_ts = last_reading.timestamp
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        ingest_ts_utc = ingest_timestamp
        if ingest_ts_utc.tzinfo is None:
            ingest_ts_utc = ingest_ts_utc.replace(tzinfo=timezone.utc)
        
        age_seconds = (ingest_ts_utc - last_ts).total_seconds()
        if age_seconds > 600:
            return None
        
        delta_info = self._delta_detector.check_delta_spike(
            sensor_id, value, ingest_timestamp, last_reading
        )
        
        if not delta_info or not delta_info.get("is_spike"):
            return None
        
        if self._delta_detector.is_in_cooldown(sensor_id, 'WARNING'):
            return ClassifiedReading(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_timestamp,
                classification=ReadingClass.ML_PREDICTION,
                reason="Delta spike en cooldown",
            )
        
        self._state_manager.transition_to(sensor_id, SensorOperationalState.WARNING)
        
        return ClassifiedReading(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
            classification=ReadingClass.WARNING,
            delta_info=delta_info,
            reason=f"Delta spike: {delta_info.get('reason', '')}",
        )

    def _violates_range(self, value: float, physical_range: PhysicalRange) -> bool:
        """Verifica si un valor viola el rango físico."""
        if physical_range.min_value is not None and value < physical_range.min_value:
            return True
        if physical_range.max_value is not None and value > physical_range.max_value:
            return True
        return False

    def _is_within_warning_band(self, value: float, th) -> bool:
        """Verifica si el valor está dentro del rango WARNING."""
        if th is None:
            return False
        if th.warning_min is not None and value < th.warning_min:
            return False
        if th.warning_max is not None and value > th.warning_max:
            return False
        if th.warning_min is None and th.warning_max is None:
            return False
        return True

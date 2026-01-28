"""Módulo de clasificación de lecturas.

Contiene la lógica de clasificación de lecturas por propósito:
- ALERT: Violación de rango físico
- WARNING: Delta spike detectado
- PREDICTION: Dato limpio para ML

También contiene el gestor de estado operacional del sensor.
"""

from .classifier import (
    ReadingClassifier,
    ReadingClass,
    ClassifiedReading,
    CanonicalThresholds,
    PhysicalRange,
    DeltaThreshold,
    LastReading,
)
from .sensor_state import (
    SensorStateManager,
    SensorOperationalState,
    SensorStateInfo,
)

__all__ = [
    "ReadingClassifier",
    "ReadingClass",
    "ClassifiedReading",
    "CanonicalThresholds",
    "PhysicalRange",
    "DeltaThreshold",
    "LastReading",
    "SensorStateManager",
    "SensorOperationalState",
    "SensorStateInfo",
]

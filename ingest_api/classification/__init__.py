"""Módulo de clasificación de lecturas por propósito.

Estructura modular:
- models.py: Dataclasses (ReadingClass, ClassifiedReading, etc.)
- thresholds.py: Gestión de umbrales desde BD
- delta_detector.py: Detección de delta spikes
- consecutive_tracker.py: Tracker de lecturas consecutivas
- reading_classifier.py: Clasificador principal
- state_models.py: Modelos de estado operacional
- state_repository.py: Acceso a BD para estados
- state_manager.py: Gestor de estado del sensor

Archivos legacy (deprecados):
- classifier.py: Usar reading_classifier.py
- sensor_state.py: Usar state_manager.py
"""

from .models import (
    ReadingClass,
    ClassifiedReading,
    PhysicalRange,
    DeltaThreshold,
    LastReading,
    CanonicalThresholds,
)
from .state_models import SensorOperationalState, SensorStateInfo
from .state_manager import SensorStateManager
from .reading_classifier import ReadingClassifier
from .thresholds import ThresholdManager
from .delta_detector import DeltaDetector
from .consecutive_tracker import ConsecutiveTracker

__all__ = [
    "ReadingClassifier",
    "ReadingClass",
    "ClassifiedReading",
    "PhysicalRange",
    "DeltaThreshold",
    "LastReading",
    "CanonicalThresholds",
    "SensorStateManager",
    "SensorOperationalState",
    "SensorStateInfo",
    "ThresholdManager",
    "DeltaDetector",
    "ConsecutiveTracker",
]

"""Modelos de datos para clasificación de lecturas.

Dataclasses que representan los diferentes tipos de datos
usados en el proceso de clasificación.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional


class ReadingClass(Enum):
    """Clasificación de una lectura según su propósito."""

    ALERT = "alert"  # Violación de rango físico
    WARNING = "warning"  # Delta spike detectado
    ML_PREDICTION = "ml_prediction"  # Dato limpio para ML


@dataclass
class CanonicalThresholds:
    """Umbrales canónicos WARNING/ALERT para subordinar semántica de delta spike."""

    warning_min: Optional[float] = None
    warning_max: Optional[float] = None
    alert_min: Optional[float] = None
    alert_max: Optional[float] = None


@dataclass
class PhysicalRange:
    """Rango físico del sensor (hard limits)."""

    min_value: Optional[float]
    max_value: Optional[float]
    threshold_id: Optional[int] = None


@dataclass
class DeltaThreshold:
    """Umbrales de detección de delta/spike."""

    abs_delta: Optional[float] = None
    rel_delta: Optional[float] = None
    abs_slope: Optional[float] = None
    rel_slope: Optional[float] = None
    severity: str = "warning"


@dataclass
class LastReading:
    """Última lectura conocida del sensor."""

    value: float
    timestamp: datetime
    reading_id: Optional[int] = None


@dataclass
class ClassifiedReading:
    """Resultado de la clasificación de una lectura."""

    sensor_id: int
    value: float
    device_timestamp: Optional[datetime]
    classification: ReadingClass
    physical_range: Optional[PhysicalRange] = None
    delta_info: Optional[dict] = None
    reason: str = ""

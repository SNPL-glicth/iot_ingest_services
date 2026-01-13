"""TAREA 4: Detector de delta spike mejorado con Z-score y ventana móvil.

Este módulo reemplaza la detección simple de umbrales fijos por un algoritmo
estadístico que considera el comportamiento histórico del sensor.

Un spike real es cuando:
- El delta actual está a más de z_threshold desviaciones del delta promedio
- O hay oscilación rápida (cambios de signo consecutivos)

Esto reduce falsos positivos en datos estables.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.engine import Connection

from iot_ingest_services.ml_service.utils.numeric_precision import safe_float, is_valid_sensor_value


@dataclass
class SpikeDetectionResult:
    """Resultado de la detección de spike."""
    is_spike: bool
    z_score: float
    oscillation_ratio: float
    mean_delta: float
    std_delta: float
    current_delta: float
    reason: str
    severity: str = "warning"


@dataclass
class SensorDeltaStats:
    """Estadísticas de delta para un sensor."""
    sensor_id: int
    mean_delta: float
    std_delta: float
    sample_count: int
    last_updated: datetime


class DeltaSpikeDetector:
    """Detector de spikes usando Z-score sobre ventana móvil."""

    # Configuración por defecto
    DEFAULT_WINDOW_SIZE = 20  # Últimas N lecturas para calcular stats
    DEFAULT_Z_THRESHOLD = 3.0  # Desviaciones estándar para considerar spike
    DEFAULT_OSCILLATION_THRESHOLD = 0.7  # Ratio de cambios de signo
    MIN_SAMPLES = 5  # Mínimo de muestras para calcular stats

    def __init__(self, db: Connection):
        self._db = db
        self._stats_cache: dict[int, SensorDeltaStats] = {}
        self._readings_cache: dict[int, List[Tuple[float, datetime]]] = {}

    def detect_spike(
        self,
        sensor_id: int,
        current_value: float,
        current_ts: datetime,
        z_threshold: Optional[float] = None,
        oscillation_threshold: Optional[float] = None,
    ) -> Optional[SpikeDetectionResult]:
        """Detecta si el valor actual representa un spike.

        Args:
            sensor_id: ID del sensor
            current_value: Valor actual de la lectura
            current_ts: Timestamp de la lectura actual
            z_threshold: Umbral de Z-score (default: 3.0)
            oscillation_threshold: Umbral de oscilación (default: 0.7)

        Returns:
            SpikeDetectionResult si se detecta spike, None si es normal
        """
        z_threshold = z_threshold or self.DEFAULT_Z_THRESHOLD
        oscillation_threshold = oscillation_threshold or self.DEFAULT_OSCILLATION_THRESHOLD

        # Obtener lecturas recientes
        recent_values = self._get_recent_values(sensor_id, self.DEFAULT_WINDOW_SIZE)

        if len(recent_values) < self.MIN_SAMPLES:
            # No hay suficientes datos para análisis estadístico
            return None

        # Calcular deltas históricos
        deltas = []
        for i in range(1, len(recent_values)):
            delta = recent_values[i][0] - recent_values[i - 1][0]
            deltas.append(delta)

        if not deltas:
            return None

        # Estadísticas de la ventana
        mean_delta = statistics.mean([abs(d) for d in deltas])
        std_delta = statistics.stdev([abs(d) for d in deltas]) if len(deltas) > 1 else 0.01

        # Delta actual
        last_value = recent_values[-1][0]
        current_delta = current_value - last_value

        # Z-score del delta actual
        if std_delta < 0.001:
            std_delta = 0.001  # Evitar división por cero
        z_score = (abs(current_delta) - mean_delta) / std_delta

        # Detección de oscilación (cambios de signo rápidos)
        sign_changes = 0
        for i in range(1, len(deltas)):
            if deltas[i] * deltas[i - 1] < 0:
                sign_changes += 1
        oscillation_ratio = sign_changes / len(deltas) if deltas else 0

        # Determinar si es spike
        is_spike = False
        reason = "normal"

        if z_score > z_threshold:
            is_spike = True
            reason = f"z_score={z_score:.2f} > {z_threshold}"
        elif oscillation_ratio > oscillation_threshold:
            is_spike = True
            reason = f"oscillation={oscillation_ratio:.2f} > {oscillation_threshold}"

        if not is_spike:
            return None

        # Determinar severidad basada en Z-score
        severity = "warning"
        if z_score > z_threshold * 2:
            severity = "critical"

        return SpikeDetectionResult(
            is_spike=True,
            z_score=z_score,
            oscillation_ratio=oscillation_ratio,
            mean_delta=mean_delta,
            std_delta=std_delta,
            current_delta=current_delta,
            reason=reason,
            severity=severity,
        )

    def _get_recent_values(
        self, sensor_id: int, window_size: int
    ) -> List[Tuple[float, datetime]]:
        """Obtiene las últimas N lecturas del sensor."""
        # Verificar cache
        if sensor_id in self._readings_cache:
            cached = self._readings_cache[sensor_id]
            if len(cached) >= window_size:
                return cached[-window_size:]

        # Consultar BD
        rows = self._db.execute(
            text(
                """
                SELECT TOP (:limit) [value], [timestamp]
                FROM dbo.sensor_readings
                WHERE sensor_id = :sensor_id
                ORDER BY [timestamp] DESC
                """
            ),
            {"sensor_id": sensor_id, "limit": window_size},
        ).fetchall()

        if not rows:
            return []

        # Convertir y filtrar valores válidos
        values = []
        for row in reversed(rows):  # Invertir para orden cronológico
            val = safe_float(row[0], None)
            if val is not None and is_valid_sensor_value(val):
                values.append((val, row[1]))

        # Actualizar cache
        self._readings_cache[sensor_id] = values

        return values

    def update_cache(self, sensor_id: int, value: float, timestamp: datetime):
        """Actualiza el cache con una nueva lectura."""
        if sensor_id not in self._readings_cache:
            self._readings_cache[sensor_id] = []

        self._readings_cache[sensor_id].append((value, timestamp))

        # Mantener solo las últimas N lecturas
        if len(self._readings_cache[sensor_id]) > self.DEFAULT_WINDOW_SIZE * 2:
            self._readings_cache[sensor_id] = self._readings_cache[sensor_id][
                -self.DEFAULT_WINDOW_SIZE :
            ]

    def clear_cache(self, sensor_id: Optional[int] = None):
        """Limpia el cache de lecturas."""
        if sensor_id is not None:
            self._readings_cache.pop(sensor_id, None)
            self._stats_cache.pop(sensor_id, None)
        else:
            self._readings_cache.clear()
            self._stats_cache.clear()


def integrate_with_classifier(
    classifier_instance,
    detector: DeltaSpikeDetector,
    sensor_id: int,
    current_value: float,
    current_ts: datetime,
) -> Optional[dict]:
    """Función de integración para usar con ReadingClassifier existente.

    Reemplaza la lógica de _check_delta_spike con el nuevo detector.

    Args:
        classifier_instance: Instancia de ReadingClassifier
        detector: Instancia de DeltaSpikeDetector
        sensor_id: ID del sensor
        current_value: Valor actual
        current_ts: Timestamp actual

    Returns:
        dict compatible con el formato de _check_delta_spike original
    """
    result = detector.detect_spike(sensor_id, current_value, current_ts)

    if result is None:
        return None

    # Actualizar cache para próximas detecciones
    detector.update_cache(sensor_id, current_value, current_ts)

    return {
        "is_spike": True,
        "delta_abs": abs(result.current_delta),
        "z_score": result.z_score,
        "oscillation_ratio": result.oscillation_ratio,
        "mean_delta": result.mean_delta,
        "std_delta": result.std_delta,
        "triggered_thresholds": ["z_score" if result.z_score > 3.0 else "oscillation"],
        "severity": result.severity,
        "reason": result.reason,
    }

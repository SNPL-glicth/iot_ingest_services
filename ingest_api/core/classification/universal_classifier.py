"""UniversalClassifier - Clasificador agnóstico de dominio.

Clasifica DataPoints basándose en StreamConfig sin conocer el dominio específico.
Trabaja en paralelo con ReadingClassifier (IoT legacy) sin interferir.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from ..domain.data_point import DataPoint
from ..domain.stream_config import StreamConfig
from ..domain.classification import ClassificationResult, DataPointClass

logger = logging.getLogger(__name__)


class UniversalClassifier:
    """Clasificador universal agnóstico de dominio.
    
    Clasifica DataPoints según StreamConfig sin conocer detalles del dominio.
    
    Orden de evaluación:
    1. Sin config → NORMAL (sin constraints)
    2. Violación física → CRITICAL_VIOLATION
    3. Violación operacional → WARNING_VIOLATION
    4. Zona crítica → CRITICAL_VIOLATION
    5. Zona warning → WARNING_VIOLATION
    6. Delta excesivo → ANOMALY_DETECTED
    7. Default → NORMAL
    """
    
    def __init__(self):
        """Inicializa el clasificador universal."""
        self._last_values = {}  # Cache de últimos valores por series_id
        self._lock = threading.Lock()  # Thread safety para acceso concurrente
    
    def classify(
        self,
        data_point: DataPoint,
        config: Optional[StreamConfig] = None,
    ) -> ClassificationResult:
        """Clasifica un DataPoint según su configuración.
        
        Args:
            data_point: DataPoint a clasificar
            config: Configuración del stream (opcional)
            
        Returns:
            ClassificationResult con la clasificación y razón
        """
        # Si no hay config, aceptar como NORMAL
        if config is None or config.constraints is None:
            return ClassificationResult.create_normal(
                data_point=data_point,
                reason="No constraints configured",
            )
        
        constraints = config.constraints
        value = data_point.value
        
        # 1. Verificar violación de rango físico (hard limit)
        if constraints.violates_physical(value):
            if constraints.physical_min is not None and value < constraints.physical_min:
                return ClassificationResult.create_critical(
                    data_point=data_point,
                    reason=f"Value {value} below physical minimum {constraints.physical_min}",
                    violated_constraint="physical_min",
                    constraint_value=constraints.physical_min,
                )
            if constraints.physical_max is not None and value > constraints.physical_max:
                return ClassificationResult.create_critical(
                    data_point=data_point,
                    reason=f"Value {value} above physical maximum {constraints.physical_max}",
                    violated_constraint="physical_max",
                    constraint_value=constraints.physical_max,
                )
        
        # 2. Verificar zona crítica
        if constraints.in_critical_zone(value):
            if constraints.critical_min is not None and value < constraints.critical_min:
                return ClassificationResult.create_critical(
                    data_point=data_point,
                    reason=f"Value {value} in critical zone (below {constraints.critical_min})",
                    violated_constraint="critical_min",
                    constraint_value=constraints.critical_min,
                )
            if constraints.critical_max is not None and value > constraints.critical_max:
                return ClassificationResult.create_critical(
                    data_point=data_point,
                    reason=f"Value {value} in critical zone (above {constraints.critical_max})",
                    violated_constraint="critical_max",
                    constraint_value=constraints.critical_max,
                )
        
        # 3. Verificar violación operacional (soft limit)
        if constraints.violates_operational(value):
            if constraints.operational_min is not None and value < constraints.operational_min:
                return ClassificationResult.create_warning(
                    data_point=data_point,
                    reason=f"Value {value} below operational minimum {constraints.operational_min}",
                    violated_constraint="operational_min",
                    constraint_value=constraints.operational_min,
                )
            if constraints.operational_max is not None and value > constraints.operational_max:
                return ClassificationResult.create_warning(
                    data_point=data_point,
                    reason=f"Value {value} above operational maximum {constraints.operational_max}",
                    violated_constraint="operational_max",
                    constraint_value=constraints.operational_max,
                )
        
        # 4. Verificar zona de advertencia
        if constraints.in_warning_zone(value):
            if constraints.warning_min is not None and value < constraints.warning_min:
                return ClassificationResult.create_warning(
                    data_point=data_point,
                    reason=f"Value {value} in warning zone (below {constraints.warning_min})",
                    violated_constraint="warning_min",
                    constraint_value=constraints.warning_min,
                )
            if constraints.warning_max is not None and value > constraints.warning_max:
                return ClassificationResult.create_warning(
                    data_point=data_point,
                    reason=f"Value {value} in warning zone (above {constraints.warning_max})",
                    violated_constraint="warning_max",
                    constraint_value=constraints.warning_max,
                )
        
        # 5. Verificar delta excesivo (cambio brusco)
        with self._lock:
            last_value = self._last_values.get(data_point.series_id)
        
        if last_value is not None:
            abs_delta = abs(value - last_value)
            
            # Delta absoluto
            if constraints.max_abs_delta is not None:
                if abs_delta > constraints.max_abs_delta:
                    with self._lock:
                        self._last_values[data_point.series_id] = value
                    return ClassificationResult.create_anomaly(
                        data_point=data_point,
                        reason=f"Absolute delta {abs_delta:.2f} exceeds threshold {constraints.max_abs_delta}",
                        confidence=0.9,
                        metadata={"delta": abs_delta, "previous_value": last_value},
                    )
            
            # Delta relativo
            if constraints.max_rel_delta is not None and last_value != 0:
                rel_delta = abs((value - last_value) / last_value) * 100
                if rel_delta > constraints.max_rel_delta:
                    with self._lock:
                        self._last_values[data_point.series_id] = value
                    return ClassificationResult.create_anomaly(
                        data_point=data_point,
                        reason=f"Relative delta {rel_delta:.2f}% exceeds threshold {constraints.max_rel_delta}%",
                        confidence=0.9,
                        metadata={"delta_percent": rel_delta, "previous_value": last_value},
                    )
        
        # Actualizar último valor
        with self._lock:
            self._last_values[data_point.series_id] = value
        
        # 6. Dato limpio para ML
        return ClassificationResult.create_normal(
            data_point=data_point,
            reason="Value within all configured ranges",
        )
    
    def reset_history(self, series_id: Optional[str] = None):
        """Resetea el historial de valores.
        
        Args:
            series_id: Si se especifica, resetea solo esa serie.
                      Si es None, resetea todo el historial.
        """
        with self._lock:
            if series_id is None:
                self._last_values.clear()
            elif series_id in self._last_values:
                del self._last_values[series_id]

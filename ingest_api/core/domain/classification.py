"""Classification - Sistema de clasificación universal agnóstico de dominio.

Define el resultado de clasificación para cualquier tipo de dato,
independiente del dominio (IoT, infraestructura, finanzas, salud, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

# Import local - evitar dependencia circular
if False:  # TYPE_CHECKING
    from .data_point import DataPoint


class DataPointClass(Enum):
    """Clasificación universal de datos.
    
    Categorías aplicables a cualquier dominio:
    - CRITICAL_VIOLATION: Dato fuera de rango físico/crítico
    - WARNING_VIOLATION: Dato fuera de rango operacional/advertencia
    - ANOMALY_DETECTED: Anomalía estadística detectada
    - NORMAL: Dato limpio, apto para ML
    - REJECTED: Dato que no debe persistirse
    """
    
    CRITICAL_VIOLATION = "critical_violation"
    WARNING_VIOLATION = "warning_violation"
    ANOMALY_DETECTED = "anomaly_detected"
    NORMAL = "normal"
    REJECTED = "rejected"


@dataclass
class ClassificationResult:
    """Resultado de clasificación agnóstico de dominio.
    
    Contiene el DataPoint clasificado junto con metadata sobre
    la decisión de clasificación.
    """
    
    data_point: Any  # DataPoint - evitar import circular
    classification: DataPointClass
    reason: str
    confidence: float = 1.0
    
    # Detalles de violación (si aplica)
    violated_constraint: Optional[str] = None
    constraint_value: Optional[float] = None
    
    # Metadata adicional
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def should_persist(self) -> bool:
        """Indica si el dato debe persistirse.
        
        Returns:
            True si el dato debe guardarse en la base de datos
        """
        return self.classification != DataPointClass.REJECTED
    
    @property
    def should_alert(self) -> bool:
        """Indica si debe generar alerta.
        
        Returns:
            True si debe crearse una alerta/notificación
        """
        return self.classification in (
            DataPointClass.CRITICAL_VIOLATION,
            DataPointClass.WARNING_VIOLATION,
        )
    
    @property
    def should_publish_to_ml(self) -> bool:
        """Indica si debe publicarse a ML.
        
        Returns:
            True si el dato es apto para predicción ML
        """
        return self.classification == DataPointClass.NORMAL
    
    @classmethod
    def create_normal(
        cls,
        data_point: Any,
        reason: str = "Dato dentro de rangos normales",
        **kwargs
    ) -> ClassificationResult:
        """Factory para resultado NORMAL.
        
        Args:
            data_point: DataPoint clasificado
            reason: Razón de la clasificación
            **kwargs: Campos adicionales
            
        Returns:
            ClassificationResult con clasificación NORMAL
        """
        return cls(
            data_point=data_point,
            classification=DataPointClass.NORMAL,
            reason=reason,
            **kwargs
        )
    
    @classmethod
    def create_critical(
        cls,
        data_point: Any,
        reason: str,
        violated_constraint: Optional[str] = None,
        constraint_value: Optional[float] = None,
        **kwargs
    ) -> ClassificationResult:
        """Factory para resultado CRITICAL_VIOLATION.
        
        Args:
            data_point: DataPoint clasificado
            reason: Razón de la violación
            violated_constraint: Nombre del constraint violado
            constraint_value: Valor del constraint
            **kwargs: Campos adicionales
            
        Returns:
            ClassificationResult con clasificación CRITICAL_VIOLATION
        """
        return cls(
            data_point=data_point,
            classification=DataPointClass.CRITICAL_VIOLATION,
            reason=reason,
            violated_constraint=violated_constraint,
            constraint_value=constraint_value,
            **kwargs
        )
    
    @classmethod
    def create_warning(
        cls,
        data_point: Any,
        reason: str,
        violated_constraint: Optional[str] = None,
        constraint_value: Optional[float] = None,
        **kwargs
    ) -> ClassificationResult:
        """Factory para resultado WARNING_VIOLATION.
        
        Args:
            data_point: DataPoint clasificado
            reason: Razón de la advertencia
            violated_constraint: Nombre del constraint violado
            constraint_value: Valor del constraint
            **kwargs: Campos adicionales
            
        Returns:
            ClassificationResult con clasificación WARNING_VIOLATION
        """
        return cls(
            data_point=data_point,
            classification=DataPointClass.WARNING_VIOLATION,
            reason=reason,
            violated_constraint=violated_constraint,
            constraint_value=constraint_value,
            **kwargs
        )
    
    @classmethod
    def create_anomaly(
        cls,
        data_point: Any,
        reason: str,
        confidence: float = 0.8,
        **kwargs
    ) -> ClassificationResult:
        """Factory para resultado ANOMALY_DETECTED.
        
        Args:
            data_point: DataPoint clasificado
            reason: Razón de la detección de anomalía
            confidence: Confianza de la detección (0-1)
            **kwargs: Campos adicionales
            
        Returns:
            ClassificationResult con clasificación ANOMALY_DETECTED
        """
        return cls(
            data_point=data_point,
            classification=DataPointClass.ANOMALY_DETECTED,
            reason=reason,
            confidence=confidence,
            **kwargs
        )
    
    @classmethod
    def create_rejected(
        cls,
        data_point: Any,
        reason: str,
        **kwargs
    ) -> ClassificationResult:
        """Factory para resultado REJECTED.
        
        Args:
            data_point: DataPoint clasificado
            reason: Razón del rechazo
            **kwargs: Campos adicionales
            
        Returns:
            ClassificationResult con clasificación REJECTED
        """
        return cls(
            data_point=data_point,
            classification=DataPointClass.REJECTED,
            reason=reason,
            **kwargs
        )

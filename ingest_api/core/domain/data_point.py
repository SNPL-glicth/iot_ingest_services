"""DataPoint - Modelo universal agnóstico de dominio.

Reemplaza Reading como contrato universal. Puede representar:
- Lectura IoT: sensor_id=42 → series_id="iot:sensor:42"
- Métrica servidor: CPU% host=web-01 → series_id="infra:web-01:cpu"
- Dato financiero: BTC price → series_id="finance:btc:price"
- Lectura médica: BPM patient=123 → series_id="health:patient:123:bpm"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional


class DataPointStatus(Enum):
    """Estado de procesamiento agnóstico."""
    PENDING = "pending"
    VALIDATED = "validated"
    CLASSIFIED = "classified"
    PERSISTED = "persisted"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass
class DataPoint:
    """Punto de dato universal - agnóstico de dominio.
    
    Este es el nuevo contrato canónico que fluye por el pipeline universal:
    Transport → Validation → Classification → Persistence → ML
    """
    
    # CAMPOS OBLIGATORIOS (mínimo viable)
    series_id: str
    value: float
    timestamp: datetime
    
    # CAMPOS OPCIONALES (enriquecimiento)
    domain: str = "generic"
    source_id: Optional[str] = None
    stream_id: Optional[str] = None
    stream_type: Optional[str] = None
    
    # METADATA FLEXIBLE
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # CONTROL DE FLUJO
    status: DataPointStatus = DataPointStatus.PENDING
    sequence: Optional[int] = None
    
    # TIMESTAMPS DE PROCESAMIENTO
    received_at: datetime = field(default_factory=datetime.utcnow)
    processed_at: Optional[datetime] = None
    
    # LEGACY COMPATIBILITY (solo para IoT adapter)
    legacy_sensor_id: Optional[int] = None
    legacy_device_id: Optional[int] = None
    
    @classmethod
    def from_series_id(
        cls,
        series_id: str,
        value: float,
        timestamp: datetime,
        **kwargs
    ) -> DataPoint:
        """Factory principal - crea DataPoint desde series_id.
        
        series_id format: "{domain}:{source}:{stream}"
        
        Ejemplos:
        - "iot:sensor:42" → sensor IoT
        - "infra:web-01:cpu" → CPU de servidor
        - "finance:binance:btc_usdt" → precio BTC
        - "health:patient:123:bpm" → BPM de paciente
        
        Args:
            series_id: Identificador único de la serie temporal
            value: Valor numérico
            timestamp: Timestamp del dato
            **kwargs: Campos adicionales opcionales
            
        Returns:
            DataPoint configurado con domain, source_id, stream_id parseados
        """
        parts = series_id.split(":", 2)
        domain = parts[0] if len(parts) > 0 else "generic"
        source_id = parts[1] if len(parts) > 1 else None
        stream_id = parts[2] if len(parts) > 2 else None
        
        return cls(
            series_id=series_id,
            value=value,
            timestamp=timestamp,
            domain=domain,
            source_id=source_id,
            stream_id=stream_id,
            **kwargs
        )
    
    def to_ml_format(self) -> Dict[str, Any]:
        """Convierte a formato para ML service (series_id: str).
        
        ML service espera:
        - series_id: str (identificador único)
        - value: float
        - timestamp: float (Unix epoch)
        - metadata: dict (opcional)
        
        Returns:
            Dict compatible con ML service
        """
        return {
            "series_id": self.series_id,
            "value": float(self.value),
            "timestamp": self.timestamp.timestamp(),
            "metadata": self.metadata,
        }
    
    def mark_classified(self):
        """Marca el DataPoint como clasificado."""
        self.status = DataPointStatus.CLASSIFIED
    
    def mark_persisted(self):
        """Marca el DataPoint como persistido."""
        self.status = DataPointStatus.PERSISTED
        self.processed_at = datetime.utcnow()
    
    def mark_rejected(self):
        """Marca el DataPoint como rechazado."""
        self.status = DataPointStatus.REJECTED
        self.processed_at = datetime.utcnow()
    
    def mark_failed(self):
        """Marca el DataPoint como fallido."""
        self.status = DataPointStatus.FAILED
        self.processed_at = datetime.utcnow()

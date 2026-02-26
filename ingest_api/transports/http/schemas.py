"""HTTP Transport Schemas - Esquemas Pydantic para ingesta HTTP universal.

IMPORTANTE: Este archivo NO modifica schemas.py IoT existente.
Define esquemas nuevos para ingesta universal (dominios no-IoT).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class DataPointIn(BaseModel):
    """Esquema de entrada para un DataPoint individual.
    
    Puede especificarse con series_id completo o con componentes separados.
    """
    
    # Opción 1: series_id completo
    series_id: Optional[str] = None
    
    # Opción 2: componentes separados
    domain: Optional[str] = None
    source_id: Optional[str] = None
    stream_id: Optional[str] = None
    
    # Datos obligatorios
    value: float
    timestamp: Optional[datetime] = None
    
    # Metadata opcional
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    sequence: Optional[int] = None
    
    @validator('series_id', 'domain', pre=True, always=True)
    def validate_series_or_components(cls, v, values):
        """Valida que se proporcione series_id O (domain + source_id + stream_id)."""
        # Esta validación se ejecuta en el modelo completo
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "series_id": "infrastructure:web-01:cpu",
                "value": 45.2,
                "timestamp": "2026-02-25T21:00:00Z",
                "metadata": {"core": 0}
            }
        }


class DataPacketIn(BaseModel):
    """Esquema de entrada para un paquete de DataPoints.
    
    Agrupa múltiples DataPoints del mismo source_id.
    """
    
    domain: str = Field(..., description="Dominio (infrastructure, finance, health, etc.)")
    source_id: str = Field(..., description="ID de la fuente de datos")
    data_points: List[DataPointIn] = Field(..., min_items=1)
    
    @validator('domain')
    def reject_iot_domain(cls, v):
        """Rechaza domain='iot' - debe usar /ingest/packets legacy."""
        if v.lower() == "iot":
            raise ValueError(
                "domain='iot' not allowed in universal ingestion. "
                "Use /ingest/packets endpoint instead."
            )
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "domain": "infrastructure",
                "source_id": "web-01",
                "data_points": [
                    {
                        "stream_id": "cpu",
                        "value": 45.2,
                        "timestamp": "2026-02-25T21:00:00Z"
                    },
                    {
                        "stream_id": "memory",
                        "value": 78.5,
                        "timestamp": "2026-02-25T21:00:00Z"
                    }
                ]
            }
        }


class DataIngestResult(BaseModel):
    """Resultado de ingesta de datos."""
    
    accepted: int = Field(..., description="Cantidad de DataPoints aceptados")
    rejected: int = Field(default=0, description="Cantidad de DataPoints rechazados")
    classifications: Dict[str, int] = Field(
        default_factory=dict,
        description="Conteo por clasificación (normal, warning, critical, etc.)"
    )
    errors: List[str] = Field(default_factory=list, description="Errores encontrados")
    
    class Config:
        json_schema_extra = {
            "example": {
                "accepted": 2,
                "rejected": 0,
                "classifications": {
                    "normal": 2
                },
                "errors": []
            }
        }

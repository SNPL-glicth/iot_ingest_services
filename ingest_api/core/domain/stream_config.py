"""StreamConfig - Configuración de streams agnóstica de dominio.

Define constraints de validación y configuración operacional para cualquier
tipo de stream de datos (IoT, infraestructura, finanzas, salud, etc.).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from abc import ABC, abstractmethod


@dataclass
class ValueConstraints:
    """Constraints de validación agnósticos de dominio.
    
    Define límites físicos, operacionales y umbrales de alerta que pueden
    aplicarse a cualquier tipo de dato numérico.
    """
    
    # Rangos físicos (hard limits - valores imposibles)
    physical_min: Optional[float] = None
    physical_max: Optional[float] = None
    
    # Rangos operacionales (soft limits - valores fuera de lo normal)
    operational_min: Optional[float] = None
    operational_max: Optional[float] = None
    
    # Umbrales de alerta
    warning_min: Optional[float] = None
    warning_max: Optional[float] = None
    critical_min: Optional[float] = None
    critical_max: Optional[float] = None
    
    # Detección de cambios bruscos
    max_abs_delta: Optional[float] = None  # Cambio absoluto máximo
    max_rel_delta: Optional[float] = None  # Cambio relativo máximo (%)
    
    # Detección de ruido
    noise_abs_threshold: Optional[float] = None
    z_score_threshold: float = 3.0
    
    # Control de eventos
    consecutive_violations_required: int = 1
    cooldown_seconds: int = 300
    
    def violates_physical(self, value: float) -> bool:
        """Verifica si un valor viola el rango físico.
        
        Args:
            value: Valor a verificar
            
        Returns:
            True si el valor está fuera del rango físico permitido
        """
        if self.physical_min is not None and value < self.physical_min:
            return True
        if self.physical_max is not None and value > self.physical_max:
            return True
        return False
    
    def violates_operational(self, value: float) -> bool:
        """Verifica si un valor viola el rango operacional.
        
        Args:
            value: Valor a verificar
            
        Returns:
            True si el valor está fuera del rango operacional normal
        """
        if self.operational_min is not None and value < self.operational_min:
            return True
        if self.operational_max is not None and value > self.operational_max:
            return True
        return False
    
    def in_warning_zone(self, value: float) -> bool:
        """Verifica si un valor está en zona de advertencia.
        
        Args:
            value: Valor a verificar
            
        Returns:
            True si el valor está en zona de advertencia
        """
        if self.warning_min is not None and value < self.warning_min:
            return True
        if self.warning_max is not None and value > self.warning_max:
            return True
        return False
    
    def in_critical_zone(self, value: float) -> bool:
        """Verifica si un valor está en zona crítica.
        
        Args:
            value: Valor a verificar
            
        Returns:
            True si el valor está en zona crítica
        """
        if self.critical_min is not None and value < self.critical_min:
            return True
        if self.critical_max is not None and value > self.critical_max:
            return True
        return False


@dataclass
class StreamConfig:
    """Configuración de stream agnóstica de dominio.
    
    Fuente de configuración (prioridad descendente):
    1. Base de datos (PostgreSQL stream_configs table)
    2. Archivo YAML/JSON (config/{domain}/{source_id}.yaml)
    3. API externa (config service)
    4. Defaults hardcoded por dominio
    """
    
    series_id: str
    domain: str
    source_id: str
    stream_id: str
    
    # Configuración de validación
    constraints: Optional[ValueConstraints] = None
    
    # Configuración de ML
    enable_ml_prediction: bool = True
    enable_alerting: bool = True
    
    # Metadata flexible por dominio
    domain_metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def create_default(
        cls,
        series_id: str,
        domain: str = "generic",
        source_id: str = "",
        stream_id: str = "",
    ) -> StreamConfig:
        """Crea una configuración por defecto sin constraints.
        
        Args:
            series_id: Identificador de la serie
            domain: Dominio del stream
            source_id: ID de la fuente
            stream_id: ID del stream
            
        Returns:
            StreamConfig con valores por defecto
        """
        return cls(
            series_id=series_id,
            domain=domain,
            source_id=source_id,
            stream_id=stream_id,
            constraints=None,
            enable_ml_prediction=True,
            enable_alerting=False,
            domain_metadata={},
        )


class StreamConfigProvider(ABC):
    """Interface para proveedores de configuración de streams.
    
    Implementaciones:
    - DatabaseConfigProvider: Lee de PostgreSQL
    - FileConfigProvider: Lee de archivos YAML/JSON
    - APIConfigProvider: Consulta API externa
    - DefaultConfigProvider: Retorna defaults hardcoded
    """
    
    @abstractmethod
    def get_config(self, series_id: str) -> Optional[StreamConfig]:
        """Obtiene configuración para un series_id.
        
        Args:
            series_id: Identificador único de la serie temporal
            
        Returns:
            StreamConfig si existe, None si no se encuentra
        """
        pass
    
    @abstractmethod
    def get_default_config(self, domain: str) -> StreamConfig:
        """Obtiene configuración default para un dominio.
        
        Args:
            domain: Dominio (iot, infrastructure, finance, health, etc.)
            
        Returns:
            StreamConfig con valores por defecto para el dominio
        """
        pass

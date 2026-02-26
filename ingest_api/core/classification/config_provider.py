"""Config providers - Proveedores de configuración de streams.

Implementaciones de StreamConfigProvider para diferentes fuentes de configuración.
"""

from __future__ import annotations

from typing import Optional

from ..domain.stream_config import StreamConfig, StreamConfigProvider, ValueConstraints


class DefaultConfigProvider(StreamConfigProvider):
    """Proveedor de configuración por defecto.
    
    Retorna configuraciones vacías sin constraints.
    Útil como fallback cuando no hay configuración específica disponible.
    """
    
    def get_config(self, series_id: str) -> Optional[StreamConfig]:
        """Obtiene configuración para un series_id.
        
        Args:
            series_id: Identificador único de la serie temporal
            
        Returns:
            None siempre (no hay configuración específica)
        """
        return None
    
    def get_default_config(self, domain: str) -> StreamConfig:
        """Obtiene configuración default para un dominio.
        
        Args:
            domain: Dominio (iot, infrastructure, finance, health, etc.)
            
        Returns:
            StreamConfig con valores por defecto sin constraints
        """
        return StreamConfig(
            series_id=f"{domain}:*:*",
            domain=domain,
            source_id="*",
            stream_id="*",
            constraints=None,
            enable_ml_prediction=True,
            enable_alerting=False,
            domain_metadata={},
        )


class HardcodedConfigProvider(StreamConfigProvider):
    """Proveedor de configuración hardcoded por dominio.
    
    Define constraints básicos por dominio para casos comunes.
    """
    
    # Configuraciones por dominio
    DOMAIN_CONFIGS = {
        "infrastructure": {
            "cpu": ValueConstraints(
                physical_min=0.0,
                physical_max=100.0,
                operational_min=0.0,
                operational_max=90.0,
                warning_max=80.0,
                critical_max=95.0,
            ),
            "memory": ValueConstraints(
                physical_min=0.0,
                physical_max=100.0,
                operational_min=0.0,
                operational_max=90.0,
                warning_max=85.0,
                critical_max=95.0,
            ),
            "disk": ValueConstraints(
                physical_min=0.0,
                physical_max=100.0,
                operational_min=0.0,
                operational_max=85.0,
                warning_max=80.0,
                critical_max=90.0,
            ),
        },
        "finance": {
            "price": ValueConstraints(
                physical_min=0.0,
                max_rel_delta=10.0,  # 10% cambio máximo
            ),
            "volume": ValueConstraints(
                physical_min=0.0,
            ),
        },
        "health": {
            "bpm": ValueConstraints(
                physical_min=30.0,
                physical_max=220.0,
                operational_min=50.0,
                operational_max=180.0,
                warning_min=45.0,
                warning_max=160.0,
                critical_min=40.0,
                critical_max=200.0,
            ),
            "spo2": ValueConstraints(
                physical_min=0.0,
                physical_max=100.0,
                operational_min=90.0,
                operational_max=100.0,
                warning_min=88.0,
                critical_min=85.0,
            ),
        },
    }
    
    def get_config(self, series_id: str) -> Optional[StreamConfig]:
        """Obtiene configuración para un series_id.
        
        Args:
            series_id: Identificador en formato "domain:source:stream"
            
        Returns:
            StreamConfig si hay configuración hardcoded, None si no
        """
        parts = series_id.split(":", 2)
        if len(parts) < 3:
            return None
        
        domain, source_id, stream_id = parts
        
        # Buscar configuración por dominio y stream
        if domain in self.DOMAIN_CONFIGS:
            if stream_id in self.DOMAIN_CONFIGS[domain]:
                constraints = self.DOMAIN_CONFIGS[domain][stream_id]
                return StreamConfig(
                    series_id=series_id,
                    domain=domain,
                    source_id=source_id,
                    stream_id=stream_id,
                    constraints=constraints,
                    enable_ml_prediction=True,
                    enable_alerting=True,
                )
        
        return None
    
    def get_default_config(self, domain: str) -> StreamConfig:
        """Obtiene configuración default para un dominio.
        
        Args:
            domain: Dominio
            
        Returns:
            StreamConfig con valores por defecto
        """
        return StreamConfig(
            series_id=f"{domain}:*:*",
            domain=domain,
            source_id="*",
            stream_id="*",
            constraints=None,
            enable_ml_prediction=True,
            enable_alerting=True,
            domain_metadata={},
        )

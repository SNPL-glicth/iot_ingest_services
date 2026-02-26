"""SeriesIdMapper - Mapeo entre diferentes formatos de identificación.

Convierte entre:
- series_id: str (formato universal: "domain:source:stream")
- sensor_id: int (formato legacy IoT)
- Otros formatos específicos de dominio
"""

from __future__ import annotations

from typing import Optional


class SeriesIdMapper:
    """Mapea entre diferentes formatos de identificación de series.
    
    ML service espera series_id: str
    IoT legacy usa sensor_id: int
    
    Este mapper permite la interoperabilidad entre sistemas.
    """
    
    @staticmethod
    def iot_sensor_to_series_id(sensor_id: int) -> str:
        """Convierte sensor_id:int a series_id:str.
        
        Args:
            sensor_id: ID numérico del sensor IoT
            
        Returns:
            series_id en formato "iot:sensor:{id}"
            
        Example:
            >>> SeriesIdMapper.iot_sensor_to_series_id(42)
            'iot:sensor:42'
        """
        return f"iot:sensor:{sensor_id}"
    
    @staticmethod
    def series_id_to_iot_sensor(series_id: str) -> Optional[int]:
        """Extrae sensor_id:int de series_id si es IoT.
        
        Args:
            series_id: Identificador de serie en formato universal
            
        Returns:
            sensor_id numérico si es IoT, None si no es IoT o formato inválido
            
        Example:
            >>> SeriesIdMapper.series_id_to_iot_sensor("iot:sensor:42")
            42
            >>> SeriesIdMapper.series_id_to_iot_sensor("infra:web-01:cpu")
            None
        """
        if not series_id.startswith("iot:sensor:"):
            return None
        
        try:
            return int(series_id.split(":")[-1])
        except (ValueError, IndexError):
            return None
    
    @staticmethod
    def build_series_id(domain: str, source: str, stream: str) -> str:
        """Construye series_id canónico desde componentes.
        
        Args:
            domain: Dominio (iot, infrastructure, finance, health, etc.)
            source: ID de la fuente (sensor, server, exchange, patient, etc.)
            stream: ID del stream (temperature, cpu, price, bpm, etc.)
            
        Returns:
            series_id en formato "{domain}:{source}:{stream}"
            
        Example:
            >>> SeriesIdMapper.build_series_id("infrastructure", "web-01", "cpu")
            'infrastructure:web-01:cpu'
        """
        return f"{domain}:{source}:{stream}"
    
    @staticmethod
    def parse_series_id(series_id: str) -> tuple[str, str, str]:
        """Parsea series_id en sus componentes.
        
        Args:
            series_id: Identificador en formato "{domain}:{source}:{stream}"
            
        Returns:
            Tupla (domain, source, stream)
            
        Example:
            >>> SeriesIdMapper.parse_series_id("finance:binance:btc_usdt")
            ('finance', 'binance', 'btc_usdt')
        """
        parts = series_id.split(":", 2)
        domain = parts[0] if len(parts) > 0 else "generic"
        source = parts[1] if len(parts) > 1 else ""
        stream = parts[2] if len(parts) > 2 else ""
        return domain, source, stream
    
    @staticmethod
    def is_iot_series(series_id: str) -> bool:
        """Verifica si un series_id pertenece al dominio IoT.
        
        Args:
            series_id: Identificador de serie
            
        Returns:
            True si es del dominio IoT
            
        Example:
            >>> SeriesIdMapper.is_iot_series("iot:sensor:42")
            True
            >>> SeriesIdMapper.is_iot_series("infra:web-01:cpu")
            False
        """
        return series_id.startswith("iot:")

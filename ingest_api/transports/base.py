"""IngestTransport - Interface base para todos los transportes de ingesta.

Define el contrato común que deben implementar HTTP, MQTT, WebSocket, CSV.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator

# Import local - evitar dependencia circular
if False:  # TYPE_CHECKING
    from ..core.domain.data_point import DataPoint


class IngestTransport(ABC):
    """Interface común para todos los transportes de ingesta.
    
    Cada transporte (HTTP, MQTT, WebSocket, CSV) implementa esta interface
    para convertir su formato nativo a DataPoints universales.
    """
    
    @abstractmethod
    def start(self) -> bool:
        """Inicia el transporte.
        
        Returns:
            True si el inicio fue exitoso, False si falló
        """
        pass
    
    @abstractmethod
    def stop(self) -> None:
        """Detiene el transporte gracefully.
        
        Debe cerrar conexiones, liberar recursos, etc.
        """
        pass
    
    @abstractmethod
    def parse_message(self, raw_message: Any) -> Iterator[Any]:  # Iterator[DataPoint]
        """Parsea mensaje nativo del transporte a DataPoint(s).
        
        Args:
            raw_message: Mensaje en formato nativo del transporte
                - HTTP: Request body (dict)
                - MQTT: MQTTMessage
                - WebSocket: WebSocket frame (dict)
                - CSV: pandas DataFrame row (dict)
        
        Yields:
            DataPoint objects
        """
        pass
    
    @property
    @abstractmethod
    def transport_name(self) -> str:
        """Nombre del transporte.
        
        Returns:
            Nombre: http, mqtt, websocket, csv
        """
        pass
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Estadísticas del transporte.
        
        Returns:
            Dict con estadísticas (mensajes procesados, errores, etc.)
        """
        return {}

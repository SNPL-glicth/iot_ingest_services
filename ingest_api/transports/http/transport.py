"""HTTP Transport - Implementación del transporte HTTP para ingesta universal."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterator

from ..base import IngestTransport
from ...core.domain.data_point import DataPoint
from ...core.domain.series_id import SeriesIdMapper
from .schemas import DataPacketIn, DataPointIn


class HTTPTransport(IngestTransport):
    """Transporte HTTP para ingesta universal.
    
    Convierte requests HTTP (DataPacketIn) a DataPoints universales.
    """
    
    def __init__(self):
        """Inicializa el transporte HTTP."""
        self._started = False
        self._messages_processed = 0
        self._errors = 0
    
    def start(self) -> bool:
        """Inicia el transporte HTTP.
        
        Returns:
            True (HTTP no requiere inicialización especial)
        """
        self._started = True
        return True
    
    def stop(self) -> None:
        """Detiene el transporte HTTP.
        
        HTTP es stateless, no requiere limpieza especial.
        """
        self._started = False
    
    def parse_message(self, raw_message: Any) -> Iterator[DataPoint]:
        """Parsea DataPacketIn a DataPoints.
        
        Args:
            raw_message: DataPacketIn o dict con datos del paquete
            
        Yields:
            DataPoint objects
        """
        # Si viene como dict, convertir a DataPacketIn
        if isinstance(raw_message, dict):
            packet = DataPacketIn(**raw_message)
        else:
            packet = raw_message
        
        for dp_in in packet.data_points:
            try:
                # Construir series_id
                if dp_in.series_id:
                    series_id = dp_in.series_id
                else:
                    # Construir desde componentes
                    domain = dp_in.domain or packet.domain
                    source_id = dp_in.source_id or packet.source_id
                    stream_id = dp_in.stream_id or ""
                    
                    if not stream_id:
                        self._errors += 1
                        continue
                    
                    series_id = SeriesIdMapper.build_series_id(domain, source_id, stream_id)
                
                # Timestamp
                timestamp = dp_in.timestamp or datetime.utcnow()
                
                # Crear DataPoint
                dp = DataPoint.from_series_id(
                    series_id=series_id,
                    value=dp_in.value,
                    timestamp=timestamp,
                    metadata=dp_in.metadata or {},
                    sequence=dp_in.sequence,
                )
                
                self._messages_processed += 1
                yield dp
                
            except Exception as e:
                self._errors += 1
                # Log error pero continuar procesando otros DataPoints
                import logging
                logging.getLogger(__name__).warning(
                    f"Error parsing DataPoint: {e}"
                )
                continue
    
    @property
    def transport_name(self) -> str:
        """Nombre del transporte.
        
        Returns:
            "http"
        """
        return "http"
    
    @property
    def stats(self) -> dict:
        """Estadísticas del transporte.
        
        Returns:
            Dict con estadísticas de procesamiento
        """
        return {
            "transport": "http",
            "started": self._started,
            "messages_processed": self._messages_processed,
            "errors": self._errors,
        }

"""Handler para ingesta de una sola lectura.

Procesa lecturas individuales usando el router centralizado.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from ...core.domain.broker_interface import IReadingBroker
from ..router import ReadingRouter


class SingleReadingHandler:
    """Handler para ingesta de lecturas individuales.
    
    Usa el ReadingRouter que delega al SP centralizado.
    """
    
    def __init__(self, db: Session, broker: IReadingBroker) -> None:
        self._db = db
        self._router = ReadingRouter(db, broker)
    
    def ingest(
        self,
        sensor_id: int,
        value: float,
        device_timestamp: Optional[datetime] = None,
    ) -> None:
        """Ingesta una sola lectura.
        
        Clasifica la lectura ANTES de persistir y la envía al pipeline correspondiente.
        
        Args:
            sensor_id: ID del sensor
            value: Valor de la lectura
            device_timestamp: Timestamp del dispositivo (opcional)
        """
        self._router.classify_and_route(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_timestamp,
        )

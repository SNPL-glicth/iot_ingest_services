"""Domain Storage Router - Enruta DataPoints al storage correcto según dominio.

Routing:
- domain='iot' → NotImplementedError (IoT usa su propio pipeline con SP)
- domain=otros → PostgreSQLStorage
"""

from __future__ import annotations

import logging
from typing import Any

from .postgres import PostgreSQLStorage

logger = logging.getLogger(__name__)


class DomainStorageRouter:
    """Router que enruta DataPoints al storage correcto según dominio.
    
    IMPORTANTE: domain='iot' NO se maneja aquí.
    IoT tiene su propio pipeline completo (ReadingRouter → SP → SQL Server).
    """
    
    def __init__(self):
        """Inicializa el router."""
        self._postgres_storage = PostgreSQLStorage()
    
    def insert(self, data_point: Any) -> bool:  # data_point: DataPoint
        """Inserta un DataPoint en el storage correcto.
        
        Args:
            data_point: DataPoint a insertar
            
        Returns:
            True si se insertó exitosamente
            
        Raises:
            NotImplementedError: Si domain='iot' (debe usar pipeline IoT)
        """
        # Bloquear domain='iot'
        if data_point.domain.lower() == "iot":
            raise NotImplementedError(
                "domain='iot' must use IoT pipeline (ReadingRouter → SP → SQL Server). "
                "Universal storage router does not handle IoT domain."
            )
        
        # Todos los demás dominios → PostgreSQL
        return self._postgres_storage.insert_data_point(data_point)
    
    def insert_batch(self, data_points: list) -> int:
        """Inserta múltiples DataPoints.
        
        Args:
            data_points: Lista de DataPoints
            
        Returns:
            Cantidad insertada exitosamente
            
        Raises:
            NotImplementedError: Si algún DataPoint tiene domain='iot'
        """
        # Verificar que ninguno sea IoT
        for dp in data_points:
            if dp.domain.lower() == "iot":
                raise NotImplementedError(
                    "domain='iot' found in batch. IoT domain must use IoT pipeline."
                )
        
        # Insertar en PostgreSQL
        return self._postgres_storage.insert_batch(data_points)

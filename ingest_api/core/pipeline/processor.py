"""Procesador principal de lecturas."""

from __future__ import annotations

import logging
from typing import Optional

from sqlalchemy.engine import Engine

from ..domain.reading import Reading
from ..validation.reading_validator import ReadingValidator
from ..redis.publisher import RedisPublisher
from .sp_executor import SPExecutor

logger = logging.getLogger(__name__)


class ReadingProcessor:
    """Procesa lecturas a través del pipeline completo.
    
    Pipeline:
    1. Validación de dominio
    2. Ejecución del SP (BD + alertas + notificaciones)
    3. Publicación a Redis (ML)
    """
    
    def __init__(
        self,
        engine: Engine,
        redis_publisher: Optional[RedisPublisher] = None,
    ):
        self._sp_executor = SPExecutor(engine)
        self._validator = ReadingValidator()
        self._redis = redis_publisher
    
    def process(self, reading: Reading) -> bool:
        """Procesa una lectura completa.
        
        Args:
            reading: Lectura adaptada del payload MQTT
            
        Returns:
            True si se procesó correctamente
        """
        # 1. Validar lectura
        is_valid, error = self._validator.validate(reading)
        if not is_valid:
            logger.warning(
                "[PROCESSOR] Validation failed: sensor_id=%d error=%s",
                reading.sensor_id,
                error,
            )
            reading.mark_failed()
            return False
        
        # 2. Ejecutar SP (BD + alertas + notificaciones + ml_events)
        sp_success = self._sp_executor.execute(reading)
        if not sp_success:
            reading.mark_failed()
            return False
        
        # 3. Publicar a Redis para ML (opcional)
        if self._redis:
            self._redis.publish(reading)
        
        reading.mark_processed()
        return True

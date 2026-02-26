"""PostgreSQL Storage - Almacenamiento agnóstico para dominios no-IoT.

IMPORTANTE: Este módulo NO toca SQL Server ni common/db.py.
Define storage completamente nuevo para dominios universales.
"""

from __future__ import annotations

import logging
import os
from typing import List, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Singleton engine
_postgres_engine: Optional[Engine] = None


def get_postgres_engine() -> Optional[Engine]:
    """Obtiene el engine de PostgreSQL (singleton).
    
    Returns:
        Engine de PostgreSQL si POSTGRES_URL está configurado, None si no
    """
    global _postgres_engine
    
    if _postgres_engine is not None:
        return _postgres_engine
    
    postgres_url = os.getenv("POSTGRES_URL")
    if not postgres_url:
        logger.warning("POSTGRES_URL not configured - universal ingestion will be disabled")
        return None
    
    try:
        _postgres_engine = create_engine(
            postgres_url,
            pool_pre_ping=True,
            pool_recycle=300,
            echo=False,
        )
        logger.info("PostgreSQL engine created successfully")
        return _postgres_engine
    except Exception as e:
        # ISO 27001: No exponer connection string en logs
        logger.exception("Failed to create PostgreSQL engine")
        return None


class PostgreSQLStorage:
    """Storage para DataPoints en PostgreSQL.
    
    Usa tabla data_points con TimescaleDB hypertable para eficiencia.
    """
    
    def __init__(self, engine: Optional[Engine] = None):
        """Inicializa el storage.
        
        Args:
            engine: Engine de PostgreSQL (opcional, usa singleton si no se provee)
        """
        self._engine = engine or get_postgres_engine()
    
    def insert_data_point(self, dp: Any) -> bool:  # dp: DataPoint
        """Inserta un DataPoint en PostgreSQL.
        
        Args:
            dp: DataPoint a insertar
            
        Returns:
            True si se insertó exitosamente, False si falló
        """
        if self._engine is None:
            logger.warning("PostgreSQL not available - cannot insert DataPoint")
            return False
        
        conn = None
        try:
            # Context manager asegura que la conexión se cierre
            with self._engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO data_points (
                            series_id, domain, source_id, stream_id,
                            value, timestamp, metadata, sequence, ingested_at
                        ) VALUES (
                            :series_id, :domain, :source_id, :stream_id,
                            :value, :timestamp, :metadata, :sequence, NOW()
                        )
                    """),
                    {
                        "series_id": dp.series_id,
                        "domain": dp.domain,
                        "source_id": dp.source_id,
                        "stream_id": dp.stream_id,
                        "value": float(dp.value),
                        "timestamp": dp.timestamp,
                        "metadata": dp.metadata or {},
                        "sequence": dp.sequence,
                    }
                )
            return True
        except Exception as e:
            logger.exception("Error inserting DataPoint")
            return False
        finally:
            # Context manager ya cierra la conexión, pero aseguramos cleanup
            if conn is not None:
                conn.close()
    
    def insert_batch(self, data_points: List[Any]) -> int:  # List[DataPoint]
        """Inserta múltiples DataPoints en batch.
        
        Args:
            data_points: Lista de DataPoints a insertar
            
        Returns:
            Cantidad de DataPoints insertados exitosamente
        """
        if self._engine is None:
            logger.warning("PostgreSQL not available - cannot insert batch")
            return 0
        
        inserted = 0
        try:
            with self._engine.begin() as conn:
                for dp in data_points:
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO data_points (
                                    series_id, domain, source_id, stream_id,
                                    value, timestamp, metadata, sequence, ingested_at
                                ) VALUES (
                                    :series_id, :domain, :source_id, :stream_id,
                                    :value, :timestamp, :metadata, :sequence, NOW()
                                )
                            """),
                            {
                                "series_id": dp.series_id,
                                "domain": dp.domain,
                                "source_id": dp.source_id,
                                "stream_id": dp.stream_id,
                                "value": float(dp.value),
                                "timestamp": dp.timestamp,
                                "metadata": dp.metadata or {},
                                "sequence": dp.sequence,
                            }
                        )
                        inserted += 1
                    except Exception as e:
                        logger.warning(f"Error inserting DataPoint in batch: {e}")
                        continue
            
            return inserted
        except Exception as e:
            logger.exception(f"Error in batch insert: {e}")
            return inserted

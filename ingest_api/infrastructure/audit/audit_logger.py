"""Audit Logger - Registro de auditoría para cumplimiento ISO 27001.

Registra cada ingesta con información de trazabilidad:
- Quién: api_key_id, client_ip
- Qué: series_id, value, classification
- Cuándo: data_timestamp, ingested_at
- Cómo: transport
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ..persistence.postgres import get_postgres_engine

logger = logging.getLogger(__name__)


class AuditLogger:
    """Logger de auditoría para ingesta universal.
    
    Escribe a tabla ingestion_audit_log en PostgreSQL.
    Si PostgreSQL no disponible, escribe a logger estructurado (fallback).
    """
    
    def __init__(self, engine: Optional[Engine] = None):
        """Inicializa el audit logger.
        
        Args:
            engine: Engine de PostgreSQL (opcional)
        """
        self._engine = engine or get_postgres_engine()
        self._fallback_logger = logging.getLogger("audit")
    
    def log_ingestion(
        self,
        data_point: Any,  # DataPoint
        classification: str,
        transport: str,
        api_key: Optional[str] = None,
        client_ip: Optional[str] = None,
        status: str = "accepted",
        error_message: Optional[str] = None,
    ) -> None:
        """Registra una ingesta en el audit log.
        
        Args:
            data_point: DataPoint ingestado
            classification: Clasificación (normal, warning, critical, etc.)
            transport: Transporte usado (http, mqtt, websocket, csv)
            api_key: API key usado (opcional)
            client_ip: IP del cliente (opcional)
            status: Estado (accepted, rejected, failed)
            error_message: Mensaje de error si falló (opcional)
        """
        # Si PostgreSQL disponible, escribir a tabla
        if self._engine is not None:
            try:
                with self._engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO ingestion_audit_log (
                                series_id, domain, source_id, value,
                                classification, transport, api_key_hash,
                                client_ip, data_timestamp, ingested_at,
                                status, error_message, metadata
                            ) VALUES (
                                :series_id, :domain, :source_id, :value,
                                :classification, :transport, :api_key_hash,
                                :client_ip, :data_timestamp, NOW(),
                                :status, :error_message, :metadata
                            )
                        """),
                        {
                            "series_id": data_point.series_id,
                            "domain": data_point.domain,
                            "source_id": data_point.source_id,
                            "value": float(data_point.value),
                            "classification": classification,
                            "transport": transport,
                            "api_key_hash": self._hash_api_key(api_key) if api_key else None,
                            "client_ip": client_ip,
                            "data_timestamp": data_point.timestamp,
                            "status": status,
                            "error_message": error_message,
                            "metadata": data_point.metadata or {},
                        }
                    )
                return
            except Exception as e:
                logger.warning(f"Failed to write to audit log table: {e}")
                # Continuar con fallback
        
        # Fallback: escribir a logger estructurado
        self._fallback_logger.info(
            "AUDIT",
            extra={
                "series_id": data_point.series_id,
                "domain": data_point.domain,
                "source_id": data_point.source_id,
                "value": data_point.value,
                "classification": classification,
                "transport": transport,
                "api_key_hash": self._hash_api_key(api_key) if api_key else None,
                "client_ip": client_ip,
                "data_timestamp": data_point.timestamp.isoformat(),
                "ingested_at": datetime.utcnow().isoformat(),
                "status": status,
                "error_message": error_message,
            }
        )
    
    @staticmethod
    def _hash_api_key(api_key: str) -> str:
        """Hash del API key para no almacenar en texto plano.
        
        Args:
            api_key: API key original
            
        Returns:
            Hash SHA256 del API key
        """
        import hashlib
        return hashlib.sha256(api_key.encode()).hexdigest()

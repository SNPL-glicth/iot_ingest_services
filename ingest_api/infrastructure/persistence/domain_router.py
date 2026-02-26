"""Domain-based persistence router.

Routes data points to the appropriate database based on domain:
- IoT domain → SQL Server (existing system, zero changes)
- All other domains → PostgreSQL + TimescaleDB
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.engine import Engine

from ...core.domain.data_point import DataPoint
from ...core.domain.classification import ClassificationResult

logger = logging.getLogger(__name__)


class DomainPersistenceRouter:
    """Routes persistence operations to the correct database based on domain.
    
    This router maintains 100% backward compatibility with the existing IoT
    system while enabling multi-domain ingestion to PostgreSQL.
    """
    
    def __init__(
        self,
        sql_server_engine: Engine,
        postgres_engine: Optional[Engine] = None,
    ):
        """Initialize the domain router.
        
        Args:
            sql_server_engine: SQL Server engine for IoT domain
            postgres_engine: PostgreSQL engine for other domains (optional)
        """
        self._sql_server = sql_server_engine
        self._postgres = postgres_engine
        
        if postgres_engine is None:
            logger.info(
                "[DomainRouter] PostgreSQL not configured - "
                "only IoT domain will be supported"
            )
    
    def get_engine(self, domain: str) -> Engine:
        """Get the appropriate database engine for a domain.
        
        Args:
            domain: Domain identifier
            
        Returns:
            Database engine for the domain
            
        Raises:
            ValueError: If domain is not IoT and PostgreSQL is not configured
        """
        if domain == "iot":
            return self._sql_server
        
        if self._postgres is None:
            raise ValueError(
                f"PostgreSQL not configured - cannot persist domain '{domain}'. "
                "Set POSTGRES_URL environment variable to enable multi-domain ingestion."
            )
        
        return self._postgres
    
    def is_domain_supported(self, domain: str) -> bool:
        """Check if a domain is supported by the router.
        
        Args:
            domain: Domain identifier
            
        Returns:
            True if domain can be persisted
        """
        if domain == "iot":
            return True
        return self._postgres is not None
    
    def save_data_point(self, data_point: DataPoint) -> None:
        """Save a data point to the appropriate database.
        
        Args:
            data_point: Data point to save
            
        Raises:
            ValueError: If domain is not supported
        """
        if not self.is_domain_supported(data_point.domain):
            raise ValueError(
                f"Domain '{data_point.domain}' not supported - "
                "PostgreSQL not configured"
            )
        
        if data_point.domain == "iot":
            self._save_iot(data_point)
        else:
            self._save_universal(data_point)
    
    def _save_iot(self, dp: DataPoint) -> None:
        """Save IoT data point using existing SQL Server logic.
        
        This method uses the EXISTING stored procedure to maintain
        100% backward compatibility with the current IoT system.
        
        Args:
            dp: Data point with domain="iot"
        """
        if not dp.has_legacy_ids:
            logger.warning(
                "[DomainRouter] IoT data point missing legacy IDs - "
                "cannot persist to SQL Server: %s",
                dp.to_dict(),
            )
            return
        
        # Use existing stored procedure - ZERO changes to IoT flow
        try:
            with self._sql_server.begin() as conn:
                conn.execute(
                    text("EXEC sp_insert_reading_and_check_threshold "
                         "@sensor_id = :sensor_id, "
                         "@value = :value, "
                         "@device_ts = :device_ts"),
                    {
                        "sensor_id": dp.legacy_stream_int,
                        "value": float(dp.value),
                        "device_ts": dp.timestamp,
                    },
                )
            
            logger.debug(
                "[DomainRouter] IoT data point saved via SP: sensor_id=%s",
                dp.legacy_stream_int,
            )
            
        except Exception as e:
            logger.exception(
                "[DomainRouter] Failed to save IoT data point: %s", e
            )
            raise
    
    def _save_universal(self, dp: DataPoint) -> None:
        """Save non-IoT data point to PostgreSQL.
        
        Args:
            dp: Data point with domain != "iot"
        """
        try:
            with self._postgres.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO data_points (
                            stream_id, source_id, domain, value, timestamp,
                            classification, sequence, domain_metadata, ingested_at
                        ) VALUES (
                            :stream_id, :source_id, :domain, :value, :timestamp,
                            :classification, :sequence, :domain_metadata, :ingested_at
                        )
                    """),
                    {
                        "stream_id": dp.stream_id,
                        "source_id": dp.source_id,
                        "domain": dp.domain,
                        "value": float(dp.value),
                        "timestamp": dp.timestamp,
                        "classification": "normal",  # Default, will be updated by classifier
                        "sequence": dp.sequence,
                        "domain_metadata": str(dp.domain_metadata) if dp.domain_metadata else "{}",
                        "ingested_at": dp.received_at,
                    },
                )
            
            logger.debug(
                "[DomainRouter] Universal data point saved: domain=%s stream=%s",
                dp.domain,
                dp.stream_id,
            )
            
        except Exception as e:
            logger.exception(
                "[DomainRouter] Failed to save universal data point: %s", e
            )
            raise
    
    def save_alert(
        self,
        data_point: DataPoint,
        result: ClassificationResult,
    ) -> None:
        """Save an alert for a data point.
        
        Args:
            data_point: Data point that triggered the alert
            result: Classification result with alert information
        """
        if data_point.domain == "iot":
            # IoT alerts are handled by the stored procedure
            # No additional action needed here
            logger.debug(
                "[DomainRouter] IoT alert handled by SP: sensor_id=%s",
                data_point.legacy_stream_int,
            )
            return
        
        if self._postgres is None:
            logger.warning(
                "[DomainRouter] Cannot save alert - PostgreSQL not configured"
            )
            return
        
        # Determine severity from classification
        severity_map = {
            "critical_violation": "CRITICAL",
            "warning_violation": "WARNING",
            "anomaly_detected": "ANOMALY",
        }
        severity = severity_map.get(
            result.classification.value,
            "WARNING",
        )
        
        try:
            with self._postgres.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO stream_alerts (
                            stream_id, source_id, domain, severity, value,
                            threshold_violated, message, triggered_at, is_active
                        ) VALUES (
                            :stream_id, :source_id, :domain, :severity, :value,
                            :threshold_violated, :message, :triggered_at, :is_active
                        )
                    """),
                    {
                        "stream_id": data_point.stream_id,
                        "source_id": data_point.source_id,
                        "domain": data_point.domain,
                        "severity": severity,
                        "value": float(data_point.value),
                        "threshold_violated": result.violated_constraint,
                        "message": result.reason,
                        "triggered_at": datetime.utcnow(),
                        "is_active": True,
                    },
                )
            
            logger.info(
                "[DomainRouter] Alert saved: domain=%s stream=%s severity=%s",
                data_point.domain,
                data_point.stream_id,
                severity,
            )
            
        except Exception as e:
            logger.exception(
                "[DomainRouter] Failed to save alert: %s", e
            )
            raise

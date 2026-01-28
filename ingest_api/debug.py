"""Funciones de debug para desarrollo y troubleshooting.

Estas funciones solo se activan con variables de entorno específicas.
NO deben usarse en producción.
"""

from __future__ import annotations

import os
import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.orm import Session


def log_db_identity(db: Session) -> None:
    """Log de identidad de BD (solo si INGEST_DEBUG_DB_IDENTITY=1)."""
    logger = logging.getLogger(__name__)
    if os.getenv("INGEST_DEBUG_DB_IDENTITY", "").strip() != "1":
        return
    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT
                      DB_NAME() AS db_name,
                      @@SERVERNAME AS server_name,
                      SUSER_SNAME() AS user_name,
                      @@SPID AS spid
                    """
                )
            )
            .mappings()
            .one()
        )
        logger.error(
            "[DB] USING_DB=%s SERVER=%s USER=%s SPID=%s",
            row.get("db_name"),
            row.get("server_name"),
            row.get("user_name"),
            row.get("spid"),
        )
    except Exception as e:
        logger.exception("[DB] Failed to log DB identity err=%s", type(e).__name__)


def should_force_persist(*, header_value: str | None) -> bool:
    """Determina si se debe forzar persistencia de prueba."""
    if os.getenv("INGEST_DEBUG_FORCE_PERSIST", "").strip() == "1":
        return True
    if not header_value:
        return False
    v = header_value.strip().lower()
    return v in ("1", "true", "yes", "on")


def force_persist_probe(
    *,
    db: Session,
    sensor_id: int,
    ingest_timestamp: datetime,
    device_timestamp: datetime | None,
) -> None:
    """Inserción de prueba para verificar transacciones."""
    logger = logging.getLogger(__name__)
    debug_value = 999999.0
    db.execute(
        text(
            """
            INSERT INTO dbo.sensor_readings (sensor_id, value, timestamp, device_timestamp)
            VALUES (:sensor_id, :value, :ts, :device_ts)
            """
        ),
        {
            "sensor_id": int(sensor_id),
            "value": float(debug_value),
            "ts": ingest_timestamp,
            "device_ts": device_timestamp,
        },
    )

    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT
                      @@TRANCOUNT AS trancount,
                      (
                        SELECT COUNT(*)
                        FROM dbo.sensor_readings
                        WHERE sensor_id = :sensor_id AND value = :value AND timestamp = :ts
                      ) AS seen
                    """
                ),
                {
                    "sensor_id": int(sensor_id),
                    "value": float(debug_value),
                    "ts": ingest_timestamp,
                },
            )
            .mappings()
            .one()
        )
        trancount = row.get("trancount")
        seen = row.get("seen")
    except Exception:
        trancount = None
        seen = None

    logger.error(
        "[DB] FORCE_PERSIST_PROBE inserted dbo.sensor_readings sensor_id=%s value=%s ts=%s trancount=%s seen_in_tx=%s",
        sensor_id,
        debug_value,
        ingest_timestamp.isoformat(),
        trancount,
        seen,
    )

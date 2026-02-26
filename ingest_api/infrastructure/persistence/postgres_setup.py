"""PostgreSQL database setup and connection management.

This module provides PostgreSQL connectivity for non-IoT domains.
PostgreSQL is OPTIONAL - if not configured, the system continues
to work with SQL Server for IoT domain only.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


_postgres_engine: Optional[Engine] = None


def get_postgres_engine() -> Optional[Engine]:
    """Get PostgreSQL engine for non-IoT domains.
    
    Returns None if PostgreSQL is not configured via POSTGRES_URL env var.
    This allows the system to gracefully degrade to IoT-only mode.
    
    Returns:
        PostgreSQL Engine or None if not configured
    """
    global _postgres_engine
    
    if _postgres_engine is not None:
        return _postgres_engine
    
    postgres_url = os.getenv("POSTGRES_URL")
    if not postgres_url:
        logger.info(
            "[PostgreSQL] POSTGRES_URL not configured - "
            "multi-domain ingestion disabled, IoT-only mode active"
        )
        return None
    
    logger.info("[PostgreSQL] Creating engine from POSTGRES_URL")
    
    try:
        _postgres_engine = create_engine(
            postgres_url,
            pool_pre_ping=True,
            pool_recycle=300,
            future=True,
        )
        
        # Test connection
        with _postgres_engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info("[PostgreSQL] Connection test OK")
        
    except Exception as e:
        logger.exception("[PostgreSQL] Connection test FAILED: %s", e)
        _postgres_engine = None
    
    return _postgres_engine


def ensure_postgres_schema(engine: Engine) -> None:
    """Ensure PostgreSQL schema exists.
    
    Creates tables if they don't exist. Safe to call multiple times.
    
    Args:
        engine: PostgreSQL engine
    """
    logger.info("[PostgreSQL] Ensuring schema exists")
    
    # Read SQL migration file
    import pathlib
    migrations_dir = pathlib.Path(__file__).parent / "migrations"
    sql_file = migrations_dir / "postgres_001.sql"
    
    if not sql_file.exists():
        logger.warning(
            "[PostgreSQL] Migration file not found: %s - skipping schema creation",
            sql_file,
        )
        return
    
    sql_content = sql_file.read_text()
    
    # Split by semicolon and execute each statement
    statements = [s.strip() for s in sql_content.split(";") if s.strip()]
    
    try:
        with engine.begin() as conn:
            for statement in statements:
                if statement:
                    conn.execute(text(statement))
        
        logger.info("[PostgreSQL] Schema creation completed successfully")
        
    except Exception as e:
        logger.exception("[PostgreSQL] Schema creation failed: %s", e)
        raise

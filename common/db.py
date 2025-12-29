from __future__ import annotations

from typing import Iterator
from urllib.parse import quote_plus
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import Settings, get_settings


logger = logging.getLogger(__name__)


def build_sqlalchemy_url(settings: Settings) -> str:
    # Use the recommended odbc_connect form.
    # This handles:
    # - passwords with special characters
    # - driver names with spaces
    # - SQL Server port syntax (SERVER=host,port)
    odbc_str = (
        f"DRIVER={{{settings.odbc_driver}}};"
        f"SERVER={settings.db_host},{settings.db_port};"
        f"DATABASE={settings.db_name};"
        f"UID={settings.db_user};"
        f"PWD={settings.db_password};"
        "TrustServerCertificate=yes;"
    )

    return f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_str)}"


def get_engine() -> Engine:
    settings = get_settings()
    url = build_sqlalchemy_url(settings)

    # Log básico de parámetros de conexión (sin contraseña)
    logger.info(
        "[DB] Crear engine SQL Server host=%s port=%s db=%s user=%s driver=%s",
        settings.db_host,
        settings.db_port,
        settings.db_name,
        settings.db_user,
        settings.odbc_driver,
    )

    engine = create_engine(url, pool_pre_ping=True, future=True)

    # Test de conexión: ayuda a ver en logs si el microservicio realmente llega a la BD
    try:
        with engine.connect() as conn:  # type: ignore[call-arg]
            conn.execute(text("SELECT 1"))
        logger.info("[DB] Test de conexión OK")
    except Exception:
        logger.exception("[DB] Test de conexión FALLÓ")

    return engine


SessionLocal = sessionmaker(bind=get_engine(), autocommit=False, autoflush=False, future=True)


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

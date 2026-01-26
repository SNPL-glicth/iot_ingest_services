from __future__ import annotations

from typing import Iterator
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from .config import Settings, get_settings

logger = logging.getLogger(__name__)


def build_sqlalchemy_url(settings: Settings) -> str:
    return (
        f"mssql+pymssql://"
        f"{settings.db_user}:{settings.db_password}"
        f"@{settings.db_host}:{settings.db_port}"
        f"/{settings.db_name}"
    )


def get_engine() -> Engine:
    settings = get_settings()
    url = build_sqlalchemy_url(settings)

    logger.info(
        "[DB] Crear engine SQL Server host=%s port=%s db=%s user=%s",
        settings.db_host,
        settings.db_port,
        settings.db_name,
        settings.db_user,
    )

    engine = create_engine(url, pool_pre_ping=True, future=True)

    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("[DB] Test de conexión OK")
    except Exception:
        logger.exception("[DB] Test de conexión FALLÓ")

    return engine


SessionLocal = sessionmaker(
    bind=get_engine(),
    autocommit=False,
    autoflush=False,
    future=True,
)


def get_db() -> Iterator[Session]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

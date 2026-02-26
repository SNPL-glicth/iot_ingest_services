"""Persistence infrastructure for multi-domain data storage."""

from .postgres_setup import get_postgres_engine, ensure_postgres_schema
from .domain_router import DomainPersistenceRouter

__all__ = [
    "get_postgres_engine",
    "ensure_postgres_schema",
    "DomainPersistenceRouter",
]

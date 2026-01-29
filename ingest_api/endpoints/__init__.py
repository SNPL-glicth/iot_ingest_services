"""Módulo de endpoints HTTP.

Contiene todos los endpoints de la API de ingesta organizados por función.
"""

from .health import router as health_router
from .sensor_status import router as sensor_status_router
from .single_ingest import router as single_ingest_router
from .batch_ingest import router as batch_ingest_router
from .packet_ingest import router as packet_ingest_router
from .diagnostics import router as diagnostics_router

__all__ = [
    "health_router",
    "sensor_status_router",
    "single_ingest_router",
    "batch_ingest_router",
    "packet_ingest_router",
    "diagnostics_router",
]

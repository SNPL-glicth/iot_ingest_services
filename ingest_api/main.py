"""IoT Ingest Service - Punto de entrada principal.

Este archivo solo contiene wiring (configuración y conexión de módulos).
NO contiene lógica de negocio.

Estructura modular:
- endpoints/     → Definición de rutas HTTP
- auth/          → Autenticación (API Key, Device Key)
- broker/        → Publicación de lecturas a ML
- queries/       → Consultas a BD
- ingest/        → Core de ingesta (handlers, router, resolver)
- debug.py       → Funciones de debug (solo desarrollo)
"""

from __future__ import annotations

import os
import logging

from fastapi import FastAPI

from iot_ingest_services.common.db import get_engine
from .batch_inserter import init_batch_inserter, shutdown_batch_inserter
from .endpoints import (
    health_router,
    sensor_status_router,
    single_ingest_router,
    batch_ingest_router,
    packet_ingest_router,
)


app = FastAPI(title="IoT Ingest Service", version="0.5.0")

app.include_router(health_router)
app.include_router(sensor_status_router)
app.include_router(single_ingest_router)
app.include_router(batch_ingest_router)
app.include_router(packet_ingest_router)


@app.on_event("startup")
async def startup_event():
    """Inicializa el BatchInserter para ingesta en lotes."""
    logger = logging.getLogger(__name__)
    try:
        engine = get_engine()
        init_batch_inserter(
            engine,
            buffer_size=int(os.getenv("BATCH_BUFFER_SIZE", "100")),
            flush_interval=float(os.getenv("BATCH_FLUSH_INTERVAL", "5.0")),
            max_batch_size=int(os.getenv("BATCH_MAX_SIZE", "500")),
        )
        logger.info("BatchInserter inicializado correctamente")
    except Exception as e:
        logger.error("Error inicializando BatchInserter: %s", e)


@app.on_event("shutdown")
async def shutdown_event():
    """Detiene el BatchInserter y hace flush de datos pendientes."""
    logger = logging.getLogger(__name__)
    try:
        shutdown_batch_inserter()
        logger.info("BatchInserter detenido correctamente")
    except Exception as e:
        logger.error("Error deteniendo BatchInserter: %s", e)

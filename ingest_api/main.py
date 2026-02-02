"""IoT Ingest Service - Punto de entrada principal.

Este archivo solo contiene wiring (configuración y conexión de módulos).
NO contiene lógica de negocio.

Estructura modular:
- endpoints/     → Definición de rutas HTTP
- auth/          → Autenticación (API Key, Device Key)
- broker/        → Publicación de lecturas a ML
- queries/       → Consultas a BD
- ingest/        → Core de ingesta (handlers, router, resolver)
- mqtt/          → Receptor MQTT (canal principal de ingesta)
- debug.py       → Funciones de debug (solo desarrollo)

FLUJO MQTT (cuando FF_MQTT_INGEST_ENABLED=true):
  Simulador/Dispositivo
  → MQTT topic: iot/sensors/{id}/readings
  → mqtt_receiver
  → mqtt_bridge
  → Redis Stream readings:raw
  → flujo actual SIN CAMBIOS

EJECUCIÓN:
  uvicorn ingest_api.main:app --reload --port 8001
"""

from __future__ import annotations

import os
import logging
from pathlib import Path

# Cargar .env del directorio padre (iot_ingest_services/)
try:
    from dotenv import load_dotenv
    _env_file = Path(__file__).resolve().parent.parent / ".env"
    if _env_file.exists():
        load_dotenv(_env_file)
except ImportError:
    pass

from fastapi import FastAPI

from iot_ingest_services.common.db import get_engine
from .batch_inserter import init_batch_inserter, shutdown_batch_inserter
from .endpoints import (
    health_router,
    sensor_status_router,
    single_ingest_router,
    batch_ingest_router,
    packet_ingest_router,
    diagnostics_router,
)

app = FastAPI(title="IoT Ingest Service", version="0.5.0")

app.include_router(health_router)
app.include_router(sensor_status_router)
app.include_router(single_ingest_router)
app.include_router(batch_ingest_router)
app.include_router(packet_ingest_router)
app.include_router(diagnostics_router)


@app.on_event("startup")
async def startup_event():
    """Inicializa el BatchInserter y opcionalmente el receptor MQTT."""
    global _mqtt_receiver
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
    
    # MQTT Ingest: Canal principal cuando está habilitado
    if os.getenv("FF_MQTT_INGEST_ENABLED", "false").lower() == "true":
        try:
            # Usar receptor simple (paho-mqtt directo → BD)
            from .mqtt.simple_receiver import start_simple_receiver, get_simple_receiver
            
            started = start_simple_receiver()
            if started:
                receiver = get_simple_receiver()
                logger.info(
                    "[MQTT] Receptor SIMPLE iniciado - escuchando iot/sensors/+/readings → BD directa"
                )
                logger.info(
                    "[MQTT] Broker: %s",
                    receiver.stats.get("broker") if receiver else "N/A"
                )
            else:
                logger.warning(
                    "[MQTT] Receptor SIMPLE no pudo iniciarse - verificar EMQX y BatchInserter"
                )
        except ImportError as e:
            logger.warning("[MQTT] Error importando simple_receiver: %s", e)
        except Exception as e:
            logger.exception("[MQTT] Error iniciando receptor MQTT: %s", e)
    else:
        logger.info("[MQTT] Ingesta MQTT deshabilitada (FF_MQTT_INGEST_ENABLED=false)")


@app.on_event("shutdown")
async def shutdown_event():
    """Detiene el BatchInserter, receptor MQTT y hace flush de datos pendientes."""
    logger = logging.getLogger(__name__)
    
    # Detener receptor MQTT simple
    try:
        from .mqtt.simple_receiver import stop_simple_receiver, get_simple_receiver
        receiver = get_simple_receiver()
        if receiver is not None:
            stats = receiver.stats
            stop_simple_receiver()
            logger.info(
                "[MQTT] Receptor SIMPLE detenido - processed=%d failed=%d",
                stats.get("messages_processed", 0),
                stats.get("messages_failed", 0),
            )
    except Exception as e:
        logger.error("[MQTT] Error deteniendo receptor MQTT: %s", e)
    
    try:
        shutdown_batch_inserter()
        logger.info("BatchInserter detenido correctamente")
    except Exception as e:
        logger.error("Error deteniendo BatchInserter: %s", e)


@app.get("/mqtt/health")
async def mqtt_health():
    """Health check del receptor MQTT."""
    try:
        from .mqtt.simple_receiver import get_simple_receiver
        receiver = get_simple_receiver()
        
        if receiver is None:
            return {
                "enabled": False,
                "reason": "FF_MQTT_INGEST_ENABLED=false or receiver not started",
            }
        
        return {
            "enabled": True,
            "type": "simple_receiver",
            "health": receiver.health_check(),
        }
    except Exception as e:
        return {"enabled": False, "error": str(e)}


@app.get("/mqtt/stats")
async def mqtt_stats():
    """Estadísticas del receptor MQTT."""
    try:
        from .mqtt.simple_receiver import get_simple_receiver
        receiver = get_simple_receiver()
        
        if receiver is None:
            return {"enabled": False}
        
        return {
            "enabled": True,
            "type": "simple_receiver",
            "stats": receiver.stats,
        }
    except Exception as e:
        return {"enabled": False, "error": str(e)}

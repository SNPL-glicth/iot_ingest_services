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

_mqtt_receiver = None
_mqtt_bridge = None


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
            from .mqtt import MQTTIngestReceiver, MQTTRedisBridge
            
            _mqtt_bridge = MQTTRedisBridge(
                dedup_enabled=True,
                dedup_ttl_seconds=int(os.getenv("MQTT_DEDUP_TTL_SECONDS", "60")),
            )
            _mqtt_receiver = MQTTIngestReceiver(_mqtt_bridge)
            
            started = await _mqtt_receiver.start()
            if started:
                logger.info(
                    "[MQTT] Receptor MQTT iniciado - escuchando iot/sensors/+/readings"
                )
            else:
                logger.warning(
                    "[MQTT] Receptor MQTT no pudo iniciarse - verificar EMQX y configuración"
                )
        except ImportError as e:
            logger.warning("[MQTT] Módulo iot_mqtt no disponible: %s", e)
        except Exception as e:
            logger.exception("[MQTT] Error iniciando receptor MQTT: %s", e)
    else:
        logger.info("[MQTT] Ingesta MQTT deshabilitada (FF_MQTT_INGEST_ENABLED=false)")


@app.on_event("shutdown")
async def shutdown_event():
    """Detiene el BatchInserter, receptor MQTT y hace flush de datos pendientes."""
    global _mqtt_receiver, _mqtt_bridge
    logger = logging.getLogger(__name__)
    
    if _mqtt_receiver is not None:
        try:
            stats = _mqtt_receiver.stats
            await _mqtt_receiver.stop()
            logger.info(
                "[MQTT] Receptor MQTT detenido - processed=%d failed=%d",
                stats.get("messages_processed", 0),
                stats.get("messages_failed", 0),
            )
        except Exception as e:
            logger.error("[MQTT] Error deteniendo receptor MQTT: %s", e)
    
    if _mqtt_bridge is not None:
        try:
            stats = _mqtt_bridge.stats
            logger.info(
                "[MQTT] Bridge stats - processed=%d deduplicated=%d failed=%d",
                stats.get("readings_processed", 0),
                stats.get("readings_deduplicated", 0),
                stats.get("readings_failed", 0),
            )
        except Exception as e:
            logger.error("[MQTT] Error obteniendo stats del bridge: %s", e)
    
    try:
        shutdown_batch_inserter()
        logger.info("BatchInserter detenido correctamente")
    except Exception as e:
        logger.error("Error deteniendo BatchInserter: %s", e)


@app.get("/mqtt/health")
async def mqtt_health():
    """Health check del receptor MQTT."""
    if _mqtt_receiver is None:
        return {
            "enabled": False,
            "reason": "FF_MQTT_INGEST_ENABLED=false or module not available",
        }
    
    receiver_health = _mqtt_receiver.health_check()
    bridge_health = _mqtt_bridge.health_check() if _mqtt_bridge else {}
    
    return {
        "enabled": True,
        "receiver": receiver_health,
        "bridge": bridge_health,
    }


@app.get("/mqtt/stats")
async def mqtt_stats():
    """Estadísticas del receptor MQTT."""
    if _mqtt_receiver is None:
        return {"enabled": False}
    
    return {
        "enabled": True,
        "receiver": _mqtt_receiver.stats,
        "bridge": _mqtt_bridge.stats if _mqtt_bridge else {},
    }

"""Health and readiness endpoints."""

import time
from fastapi import APIRouter, HTTPException
from sqlalchemy import text

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    """Liveness probe — always returns ok if process is running."""
    return {"status": "ok"}


@router.get("/ready")
def ready():
    """Readiness probe — checks DB connectivity."""
    try:
        from iot_ingest_services.common.db import get_engine
        from sqlalchemy import text
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception:
        raise HTTPException(status_code=503, detail="not ready")


@router.get("/health/postgres")
def postgres_health():
    """Health check para PostgreSQL (universal ingestion).
    
    Verifica conectividad con SELECT 1 y mide latencia.
    ISO 27001: No expone detalles de error al cliente.
    
    Returns:
        {"status": "ok|error|disabled", "latency_ms": float}
    """
    try:
        from ..infrastructure.persistence.postgres import get_postgres_engine
        
        engine = get_postgres_engine()
        if engine is None:
            return {
                "status": "disabled",
                "message": "PostgreSQL not configured (POSTGRES_URL not set)"
            }
        
        # Medir latencia
        start_time = time.time()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        latency_ms = (time.time() - start_time) * 1000
        
        return {
            "status": "ok",
            "latency_ms": round(latency_ms, 2)
        }
        
    except Exception as e:
        # ISO 27001: No exponer detalles del error al cliente
        # Solo loguear internamente
        import logging
        logging.getLogger(__name__).exception("PostgreSQL health check failed")
        
        return {
            "status": "error",
            "message": "PostgreSQL connection failed"
        }


@router.get("/metrics")
def metrics():
    """Aggregated metrics from ML MetricsCollector + async processor."""
    result: dict = {}
    try:
        from iot_machine_learning.ml_service.metrics.performance_metrics import (
            MetricsCollector,
        )
        result["ml"] = MetricsCollector.get_instance().get_metrics().to_dict()
    except Exception:
        result["ml"] = None
    try:
        from ..mqtt.simple_receiver import get_simple_receiver
        recv = get_simple_receiver()
        if recv and hasattr(recv, "_async_processor") and recv._async_processor:
            result["async_processor"] = recv._async_processor.metrics
    except Exception:
        pass
    return result

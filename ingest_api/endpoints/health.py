"""Health and readiness endpoints."""

from fastapi import APIRouter, HTTPException

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

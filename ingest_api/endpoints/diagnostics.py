"""Diagnostics endpoint for ingestion observability.

Provides real-time metrics for auditing ingestion precision:
- Temporal precision (delta between consecutive readings)
- Latency tracking (ingested_ts - sensor_ts)
- Order validation (out-of-order detection)

ISO 27001: Only aggregated metrics exposed, no sensitive data.
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Query

from ..metrics import get_ingestion_metrics

router = APIRouter(tags=["diagnostics"])
logger = logging.getLogger(__name__)


@router.get("/api/ingestion/diagnostics")
def get_ingestion_diagnostics(
    sensor_id: Optional[int] = Query(None, description="Filter by sensor ID"),
):
    """Get ingestion diagnostics and timing metrics.
    
    Returns real-time metrics for auditing ingestion precision:
    - Total readings processed
    - Average delta between consecutive sensor timestamps
    - Average and max latency (ingested_ts - sensor_ts)
    - Out-of-order count
    - Health assessment (PASS/WARN/FAIL)
    
    Args:
        sensor_id: Optional sensor ID to filter metrics
        
    Returns:
        JSON with diagnostics report
        
    Example response:
    ```json
    {
        "timestamp": "2026-01-29T12:00:00.000Z",
        "uptime_seconds": 3600.5,
        "summary": {
            "total_readings": 3600,
            "total_sensors": 5,
            "total_out_of_order": 0,
            "avg_delta_sensor_ms": 1002.3,
            "avg_lag_ms": 45.2,
            "max_lag_ms": 156.8
        },
        "health": "PASS",
        "health_reasons": ["All metrics within acceptable thresholds"],
        "sensors": { ... }
    }
    ```
    """
    metrics_service = get_ingestion_metrics()
    return metrics_service.get_diagnostics(sensor_id=sensor_id)


@router.get("/api/ingestion/diagnostics/{sensor_id}")
def get_sensor_diagnostics(sensor_id: int):
    """Get diagnostics for a specific sensor.
    
    Args:
        sensor_id: Sensor ID to get diagnostics for
        
    Returns:
        JSON with sensor-specific diagnostics
    """
    metrics_service = get_ingestion_metrics()
    return metrics_service.get_diagnostics(sensor_id=sensor_id)


@router.get("/api/ingestion/health")
def get_ingestion_health():
    """Get quick health check for ingestion service.
    
    Returns:
        JSON with health status
        
    Example response:
    ```json
    {
        "status": "healthy",
        "health": "PASS",
        "total_readings": 3600,
        "avg_lag_ms": 45.2,
        "out_of_order_rate": 0.0
    }
    ```
    """
    metrics_service = get_ingestion_metrics()
    metrics = metrics_service.get_metrics()
    
    out_of_order_rate = (
        metrics.total_out_of_order / metrics.total_readings
        if metrics.total_readings > 0 else 0.0
    )
    
    return {
        "status": "healthy" if metrics.health == "PASS" else "degraded",
        "health": metrics.health,
        "total_readings": metrics.total_readings,
        "total_sensors": metrics.total_sensors,
        "avg_lag_ms": metrics.avg_lag_ms,
        "max_lag_ms": metrics.max_lag_ms,
        "out_of_order_rate": round(out_of_order_rate, 4),
        "health_reasons": metrics.health_reasons,
    }

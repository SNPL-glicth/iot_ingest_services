"""Data models for ingestion metrics.

Extracted from ingestion_metrics.py for modularity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, List


@dataclass
class IngestionMetrics:
    """Aggregated ingestion metrics across all sensors."""
    
    timestamp: str
    uptime_seconds: float
    total_readings: int
    total_sensors: int
    total_out_of_order: int
    
    # Aggregated timing
    avg_delta_sensor_ms: Optional[float]
    avg_lag_ms: Optional[float]
    max_lag_ms: Optional[float]
    
    # Health assessment
    health: str  # "PASS" | "WARN" | "FAIL"
    health_reasons: List[str]
    
    # Per-sensor breakdown
    sensors: Dict[int, dict]

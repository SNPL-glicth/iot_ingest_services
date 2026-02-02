"""Metrics module for ingestion observability.

Refactorizado 2026-02-02: Separado en módulos <180 líneas.
"""

from .sensor_stats import SensorTimingStats
from .aggregator import (
    IngestionMetricsService,
    IngestionMetrics,
    get_ingestion_metrics,
)

__all__ = [
    "IngestionMetricsService",
    "IngestionMetrics",
    "SensorTimingStats",
    "get_ingestion_metrics",
]

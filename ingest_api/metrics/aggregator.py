"""Agregador de métricas de ingesta.

Módulo separado de ingestion_metrics.py para mantener archivos <180 líneas.
"""

from __future__ import annotations

import time
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, List
from statistics import mean

from .sensor_stats import SensorTimingStats

logger = logging.getLogger(__name__)


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


class IngestionMetricsService:
    """Service for tracking and exposing ingestion metrics.
    
    Thread-safe singleton that tracks timing metrics for all sensors.
    """
    
    _instance: Optional["IngestionMetricsService"] = None
    _lock = threading.Lock()
    
    # Thresholds
    MAX_ACCEPTABLE_LAG_MS = 200.0
    MAX_ACCEPTABLE_OUT_OF_ORDER_RATE = 0.01  # 1%
    
    def __init__(self):
        self._sensors: Dict[int, SensorTimingStats] = {}
        self._start_time = time.time()
        self._total_readings = 0
        self._data_lock = threading.Lock()
    
    @classmethod
    def get_instance(cls) -> "IngestionMetricsService":
        """Get singleton instance."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """Reset singleton (for testing)."""
        with cls._lock:
            cls._instance = None
    
    def record_reading(
        self,
        sensor_id: int,
        ingested_ts: float,
        sensor_ts: Optional[float] = None,
        sequence: Optional[int] = None,
    ) -> dict:
        """Record a reading and return timing metrics."""
        with self._data_lock:
            if sensor_id not in self._sensors:
                self._sensors[sensor_id] = SensorTimingStats(sensor_id=sensor_id)
            
            stats = self._sensors[sensor_id]
            result = stats.record_reading(
                sensor_ts=sensor_ts,
                ingested_ts=ingested_ts,
                sequence=sequence,
            )
            
            self._total_readings += 1
            self._log_timing(sensor_id, sensor_ts, result)
            
            return result
    
    def _log_timing(self, sensor_id: int, sensor_ts: Optional[float], result: dict):
        """Log timing metrics for observability."""
        if sensor_ts is None:
            return
        
        lag_ms = result.get("lag_ms", 0)
        delta_ms = result.get("delta_sensor_ms")
        
        if lag_ms > self.MAX_ACCEPTABLE_LAG_MS:
            logger.warning(
                "HIGH_LAG sensor_id=%s lag_ms=%.2f threshold_ms=%.2f",
                sensor_id, lag_ms, self.MAX_ACCEPTABLE_LAG_MS
            )
        
        if delta_ms is not None:
            logger.debug(
                "TIMING sensor_id=%s delta_ms=%.2f lag_ms=%.2f seq=%s",
                sensor_id, delta_ms, lag_ms, result.get("sequence")
            )
    
    def get_sensor_stats(self, sensor_id: int) -> Optional[dict]:
        """Get statistics for a specific sensor."""
        with self._data_lock:
            stats = self._sensors.get(sensor_id)
            if stats is None:
                return None
            return stats.get_stats()
    
    def get_metrics(self) -> IngestionMetrics:
        """Get aggregated metrics across all sensors."""
        with self._data_lock:
            return self._build_metrics()
    
    def _build_metrics(self) -> IngestionMetrics:
        """Build aggregated metrics (must hold lock)."""
        now = time.time()
        uptime = now - self._start_time
        
        sensors_data = {}
        all_delta_samples = []
        all_lag_samples = []
        total_out_of_order = 0
        
        for sensor_id, stats in self._sensors.items():
            sensor_stats = stats.get_stats()
            sensors_data[sensor_id] = sensor_stats
            
            total_out_of_order += stats.out_of_order_count
            all_delta_samples.extend(stats.delta_sensor_samples)
            all_lag_samples.extend(stats.lag_samples)
        
        avg_delta = round(mean(all_delta_samples), 2) if all_delta_samples else None
        avg_lag = round(mean(all_lag_samples), 2) if all_lag_samples else None
        max_lag = round(max(all_lag_samples), 2) if all_lag_samples else None
        
        health, health_reasons = self._assess_health(max_lag, total_out_of_order)
        
        return IngestionMetrics(
            timestamp=datetime.now(timezone.utc).isoformat(),
            uptime_seconds=round(uptime, 2),
            total_readings=self._total_readings,
            total_sensors=len(self._sensors),
            total_out_of_order=total_out_of_order,
            avg_delta_sensor_ms=avg_delta,
            avg_lag_ms=avg_lag,
            max_lag_ms=max_lag,
            health=health,
            health_reasons=health_reasons,
            sensors=sensors_data,
        )
    
    def _assess_health(self, max_lag: Optional[float], total_out_of_order: int) -> tuple:
        """Assess health status based on metrics."""
        health = "PASS"
        health_reasons = []
        
        if max_lag is not None and max_lag > self.MAX_ACCEPTABLE_LAG_MS:
            health = "WARN"
            health_reasons.append(f"Max lag {max_lag}ms exceeds {self.MAX_ACCEPTABLE_LAG_MS}ms")
        
        if self._total_readings > 0:
            rate = total_out_of_order / self._total_readings
            if rate > self.MAX_ACCEPTABLE_OUT_OF_ORDER_RATE:
                health = "FAIL"
                health_reasons.append(f"Out-of-order rate {rate:.2%} exceeds {self.MAX_ACCEPTABLE_OUT_OF_ORDER_RATE:.2%}")
        
        if not health_reasons:
            health_reasons.append("All metrics within acceptable thresholds")
        
        return health, health_reasons


def get_ingestion_metrics() -> IngestionMetricsService:
    """Get the ingestion metrics service singleton."""
    return IngestionMetricsService.get_instance()

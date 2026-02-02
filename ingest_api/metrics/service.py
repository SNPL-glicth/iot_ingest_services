"""Ingestion metrics service.

Extracted from ingestion_metrics.py for modularity.
"""

from __future__ import annotations

import time
import logging
import threading
from datetime import datetime, timezone
from typing import Dict, Optional
from statistics import mean

from .timing_stats import SensorTimingStats
from .models import IngestionMetrics

logger = logging.getLogger(__name__)


class IngestionMetricsService:
    """Service for tracking and exposing ingestion metrics.
    
    Thread-safe singleton that tracks timing metrics for all sensors.
    """
    
    _instance: Optional["IngestionMetricsService"] = None
    _lock = threading.Lock()
    
    def __init__(self):
        self._sensors: Dict[int, SensorTimingStats] = {}
        self._start_time = time.time()
        self._total_readings = 0
        self._data_lock = threading.Lock()
        
        # Thresholds for health assessment
        self._max_acceptable_lag_ms = 200.0
        self._max_acceptable_out_of_order_rate = 0.01  # 1%
    
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
            
            # Log timing at INFO level for observability
            if sensor_ts is not None:
                lag_ms = result.get("lag_ms", 0)
                delta_ms = result.get("delta_sensor_ms")
                
                if lag_ms > self._max_acceptable_lag_ms:
                    logger.warning(
                        "HIGH_LAG sensor_id=%s lag_ms=%.2f threshold_ms=%.2f",
                        sensor_id, lag_ms, self._max_acceptable_lag_ms
                    )
                
                if delta_ms is not None:
                    logger.debug(
                        "TIMING sensor_id=%s delta_ms=%.2f lag_ms=%.2f seq=%s",
                        sensor_id, delta_ms, lag_ms, sequence
                    )
            
            return result
    
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
            now = time.time()
            uptime = now - self._start_time
            
            # Aggregate per-sensor stats
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
            
            # Calculate aggregates
            avg_delta = round(mean(all_delta_samples), 2) if all_delta_samples else None
            avg_lag = round(mean(all_lag_samples), 2) if all_lag_samples else None
            max_lag = round(max(all_lag_samples), 2) if all_lag_samples else None
            
            # Health assessment
            health, health_reasons = self._assess_health(
                max_lag, total_out_of_order
            )
            
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
    
    def _assess_health(
        self, max_lag: Optional[float], total_out_of_order: int
    ) -> tuple:
        """Assess system health based on metrics."""
        health = "PASS"
        health_reasons = []
        
        if max_lag is not None and max_lag > self._max_acceptable_lag_ms:
            health = "WARN"
            health_reasons.append(
                f"Max lag {max_lag}ms exceeds threshold {self._max_acceptable_lag_ms}ms"
            )
        
        if self._total_readings > 0:
            out_of_order_rate = total_out_of_order / self._total_readings
            if out_of_order_rate > self._max_acceptable_out_of_order_rate:
                health = "FAIL"
                health_reasons.append(
                    f"Out-of-order rate {out_of_order_rate:.2%} exceeds threshold "
                    f"{self._max_acceptable_out_of_order_rate:.2%}"
                )
        
        if not health_reasons:
            health_reasons.append("All metrics within acceptable thresholds")
        
        return health, health_reasons
    
    def get_diagnostics(self, sensor_id: Optional[int] = None) -> dict:
        """Get diagnostics report for API endpoint."""
        metrics = self.get_metrics()
        
        result = {
            "timestamp": metrics.timestamp,
            "uptime_seconds": metrics.uptime_seconds,
            "summary": {
                "total_readings": metrics.total_readings,
                "total_sensors": metrics.total_sensors,
                "total_out_of_order": metrics.total_out_of_order,
                "avg_delta_sensor_ms": metrics.avg_delta_sensor_ms,
                "avg_lag_ms": metrics.avg_lag_ms,
                "max_lag_ms": metrics.max_lag_ms,
            },
            "health": metrics.health,
            "health_reasons": metrics.health_reasons,
        }
        
        if sensor_id is not None:
            sensor_stats = metrics.sensors.get(sensor_id)
            if sensor_stats:
                result["sensor"] = sensor_stats
            else:
                result["sensor"] = None
                result["error"] = f"Sensor {sensor_id} not found"
        else:
            result["sensors"] = metrics.sensors
        
        return result


def get_ingestion_metrics() -> IngestionMetricsService:
    """Get the ingestion metrics service singleton."""
    return IngestionMetricsService.get_instance()

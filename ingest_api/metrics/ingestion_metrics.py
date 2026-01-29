"""Ingestion metrics service for observability and diagnostics.

This module provides real-time metrics for ingestion precision:
- Temporal precision (delta between consecutive readings)
- Latency tracking (ingested_ts - sensor_ts)
- Order validation (out-of-order detection)
- Packet loss estimation

ISO 27001: Metrics are aggregated, no sensitive data exposed.
"""

from __future__ import annotations

import time
import logging
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, List
from collections import deque
from statistics import mean, stdev

logger = logging.getLogger(__name__)


@dataclass
class SensorTimingStats:
    """Timing statistics for a single sensor."""
    
    sensor_id: int
    
    # Last known values
    last_sensor_ts: Optional[float] = None
    last_ingested_ts: Optional[float] = None
    last_sequence: Optional[int] = None
    
    # Accumulated metrics
    total_readings: int = 0
    out_of_order_count: int = 0
    
    # Delta tracking (sensor_ts[n] - sensor_ts[n-1])
    delta_sensor_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    
    # Lag tracking (ingested_ts - sensor_ts)
    lag_samples: deque = field(default_factory=lambda: deque(maxlen=100))
    
    # Expected interval
    expected_interval_sec: float = 1.0
    tolerance_sec: float = 0.1
    
    def record_reading(
        self,
        sensor_ts: Optional[float],
        ingested_ts: float,
        sequence: Optional[int] = None,
    ) -> dict:
        """Record a reading and return timing analysis.
        
        Returns dict with timing metrics for this reading.
        """
        result = {
            "sensor_id": self.sensor_id,
            "sensor_ts": sensor_ts,
            "ingested_ts": ingested_ts,
            "sequence": sequence,
            "delta_sensor_ms": None,
            "lag_ms": None,
            "within_tolerance": True,
            "out_of_order": False,
        }
        
        # Calculate lag (ingested_ts - sensor_ts)
        if sensor_ts is not None:
            lag_ms = (ingested_ts - sensor_ts) * 1000
            result["lag_ms"] = lag_ms
            self.lag_samples.append(lag_ms)
        
        # Calculate delta between consecutive sensor_ts
        if sensor_ts is not None and self.last_sensor_ts is not None:
            delta_sec = sensor_ts - self.last_sensor_ts
            delta_ms = delta_sec * 1000
            result["delta_sensor_ms"] = delta_ms
            self.delta_sensor_samples.append(delta_ms)
            
            # Check tolerance
            expected_min = (self.expected_interval_sec - self.tolerance_sec) * 1000
            expected_max = (self.expected_interval_sec + self.tolerance_sec) * 1000
            result["within_tolerance"] = expected_min <= delta_ms <= expected_max
        
        # Check sequence order
        if sequence is not None and self.last_sequence is not None:
            if sequence <= self.last_sequence:
                result["out_of_order"] = True
                self.out_of_order_count += 1
                logger.warning(
                    "OUT_OF_ORDER sensor_id=%s current_seq=%s last_seq=%s",
                    self.sensor_id, sequence, self.last_sequence
                )
        
        # Update state
        if sensor_ts is not None:
            self.last_sensor_ts = sensor_ts
        self.last_ingested_ts = ingested_ts
        if sequence is not None:
            self.last_sequence = sequence
        self.total_readings += 1
        
        return result
    
    def get_stats(self) -> dict:
        """Get accumulated statistics for this sensor."""
        delta_list = list(self.delta_sensor_samples)
        lag_list = list(self.lag_samples)
        
        return {
            "sensor_id": self.sensor_id,
            "total_readings": self.total_readings,
            "out_of_order_count": self.out_of_order_count,
            "delta_sensor": {
                "avg_ms": round(mean(delta_list), 2) if delta_list else None,
                "min_ms": round(min(delta_list), 2) if delta_list else None,
                "max_ms": round(max(delta_list), 2) if delta_list else None,
                "std_ms": round(stdev(delta_list), 2) if len(delta_list) > 1 else None,
                "samples": len(delta_list),
            },
            "lag": {
                "avg_ms": round(mean(lag_list), 2) if lag_list else None,
                "min_ms": round(min(lag_list), 2) if lag_list else None,
                "max_ms": round(max(lag_list), 2) if lag_list else None,
                "std_ms": round(stdev(lag_list), 2) if len(lag_list) > 1 else None,
                "samples": len(lag_list),
            },
            "expected_interval_ms": self.expected_interval_sec * 1000,
            "tolerance_ms": self.tolerance_sec * 1000,
        }


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
    
    Usage:
        service = IngestionMetricsService.get_instance()
        service.record_reading(sensor_id=1, sensor_ts=..., ingested_ts=...)
        metrics = service.get_metrics()
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
        """Record a reading and return timing metrics.
        
        Args:
            sensor_id: ID of the sensor
            ingested_ts: When ingestion received the reading (Unix epoch)
            sensor_ts: When sensor generated the reading (Unix epoch, optional)
            sequence: Sequence number for ordering (optional)
            
        Returns:
            Dict with timing metrics for this reading
        """
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
                    logger.info(
                        "TIMING sensor_id=%s delta_ms=%.2f lag_ms=%.2f seq=%s order_ok=%s",
                        sensor_id, delta_ms, lag_ms, sequence, not result.get("out_of_order", False)
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
        from datetime import datetime, timezone
        
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
            health = "PASS"
            health_reasons = []
            
            if max_lag is not None and max_lag > self._max_acceptable_lag_ms:
                health = "WARN"
                health_reasons.append(f"Max lag {max_lag}ms exceeds threshold {self._max_acceptable_lag_ms}ms")
            
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
    
    def get_diagnostics(self, sensor_id: Optional[int] = None) -> dict:
        """Get diagnostics report for API endpoint.
        
        Args:
            sensor_id: Optional sensor ID to filter by
            
        Returns:
            Dict suitable for JSON response
        """
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
            # Return specific sensor
            sensor_stats = metrics.sensors.get(sensor_id)
            if sensor_stats:
                result["sensor"] = sensor_stats
            else:
                result["sensor"] = None
                result["error"] = f"Sensor {sensor_id} not found"
        else:
            # Return all sensors
            result["sensors"] = metrics.sensors
        
        return result


# Singleton accessor
def get_ingestion_metrics() -> IngestionMetricsService:
    """Get the ingestion metrics service singleton."""
    return IngestionMetricsService.get_instance()

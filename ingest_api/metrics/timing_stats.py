"""Timing statistics for individual sensors.

Extracted from ingestion_metrics.py for modularity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional
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
        """Record a reading and return timing analysis."""
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

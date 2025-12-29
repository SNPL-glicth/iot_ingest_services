"""Utilidades para detección de delta spikes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class DeltaThreshold:
    """Umbrales de detección de delta/spike."""

    abs_delta: Optional[float] = None
    rel_delta: Optional[float] = None
    abs_slope: Optional[float] = None
    rel_slope: Optional[float] = None
    severity: str = "warning"


@dataclass
class LastReading:
    """Última lectura conocida del sensor."""

    value: float
    timestamp: datetime
    reading_id: Optional[int] = None


def get_delta_threshold(db: Session, sensor_id: int) -> Optional[DeltaThreshold]:
    """Obtiene los umbrales de delta para el sensor."""
    row = db.execute(
        text(
            """
            SELECT TOP 1
                abs_delta,
                rel_delta,
                abs_slope,
                rel_slope,
                severity
            FROM dbo.delta_thresholds
            WHERE sensor_id = :sensor_id
              AND is_active = 1
            ORDER BY id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return DeltaThreshold(
        abs_delta=float(row.abs_delta) if row.abs_delta is not None else None,
        rel_delta=float(row.rel_delta) if row.rel_delta is not None else None,
        abs_slope=float(row.abs_slope) if row.abs_slope is not None else None,
        rel_slope=float(row.rel_slope) if row.rel_slope is not None else None,
        severity=str(row.severity or "warning"),
    )


def get_last_reading(db: Session, sensor_id: int) -> Optional[LastReading]:
    """Obtiene la última lectura del sensor desde sensor_readings_latest."""
    row = db.execute(
        text(
            """
            SELECT TOP 1
                latest_value,
                latest_timestamp
            FROM dbo.sensor_readings_latest
            WHERE sensor_id = :sensor_id
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return LastReading(
        value=float(row.latest_value),
        timestamp=row.latest_timestamp,
    )


def check_delta_spike(
    current_value: float,
    current_ts: datetime,
    last_reading: LastReading,
    delta_threshold: DeltaThreshold,
) -> Optional[dict]:
    """Verifica si hay un delta spike usando umbrales de delta/slope.

    Calcula:
    - Delta absoluto: |current - last|
    - Delta relativo: |delta| / |last|
    - Slope absoluto: |delta| / dt (unidades/segundo)
    - Slope relativo: (|delta|/|last|) / dt (1/segundo)

    Returns:
        dict con is_spike=True si se detecta spike, None en caso contrario
    """
    # Calcular delta y dt
    delta_abs = abs(current_value - last_reading.value)
    dt_seconds = (current_ts - last_reading.timestamp).total_seconds()

    # Evitar división por cero o dt negativo
    if dt_seconds <= 0:
        dt_seconds = 0.001  # 1ms mínimo para cálculos

    delta_rel = 0.0
    if abs(last_reading.value) > 1e-6:
        delta_rel = abs(delta_abs / last_reading.value)

    slope_abs = delta_abs / dt_seconds
    slope_rel = delta_rel / dt_seconds if dt_seconds > 0 else 0.0

    # Verificar umbrales
    triggered = []
    reason_parts = []

    if delta_threshold.abs_delta is not None and delta_abs >= delta_threshold.abs_delta:
        triggered.append("abs_delta")
        reason_parts.append(f"delta_abs={delta_abs:.4f} >= {delta_threshold.abs_delta:.4f}")

    if delta_threshold.rel_delta is not None and delta_rel >= delta_threshold.rel_delta:
        triggered.append("rel_delta")
        reason_parts.append(f"delta_rel={delta_rel:.4%} >= {delta_threshold.rel_delta:.4%}")

    if delta_threshold.abs_slope is not None and slope_abs >= delta_threshold.abs_slope:
        triggered.append("abs_slope")
        reason_parts.append(f"slope_abs={slope_abs:.4f} >= {delta_threshold.abs_slope:.4f}")

    if delta_threshold.rel_slope is not None and slope_rel >= delta_threshold.rel_slope:
        triggered.append("rel_slope")
        reason_parts.append(f"slope_rel={slope_rel:.4f} >= {delta_threshold.rel_slope:.4f}")

    if not triggered:
        return None

    return {
        "is_spike": True,
        "delta_abs": delta_abs,
        "delta_rel": delta_rel,
        "slope_abs": slope_abs,
        "slope_rel": slope_rel,
        "dt_seconds": dt_seconds,
        "last_value": last_reading.value,
        "triggered_thresholds": triggered,
        "severity": delta_threshold.severity,
        "reason": "; ".join(reason_parts),
    }


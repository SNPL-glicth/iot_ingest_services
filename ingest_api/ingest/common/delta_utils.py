"""Utilidades para detección de delta spikes."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
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
    is_spike: bool = False  # Bug 1.4: Marcar si fue spike para evitar efecto rebote


def _to_utc_aware(ts: datetime) -> datetime:
    if ts.tzinfo is None or ts.tzinfo.utcoffset(ts) is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


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


def get_last_reading(db: Session, sensor_id: int, max_age_seconds: int = 600) -> Optional[LastReading]:
    """Obtiene la última lectura del sensor desde sensor_readings_latest.
    
    FIX CRÍTICO: Solo retorna lectura si es RECIENTE (dentro de max_age_seconds).
    Sin historial válido → NO existe contexto para evaluar delta spike.
    
    Args:
        db: Sesión de base de datos
        sensor_id: ID del sensor
        max_age_seconds: Edad máxima en segundos para considerar válida (default: 600 = 10 min)
        
    Returns:
        LastReading si existe y es reciente, None si no existe o es muy vieja
    """
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

    # FIX RAÍZ: Validar que la lectura sea reciente
    last_ts = row.latest_timestamp
    if last_ts is None:
        return None
    
    # Normalizar a UTC para comparación
    if last_ts.tzinfo is None:
        last_ts = last_ts.replace(tzinfo=timezone.utc)
    
    now_utc = datetime.now(timezone.utc)
    age_seconds = (now_utc - last_ts).total_seconds()
    
    if age_seconds > max_age_seconds:
        # Lectura demasiado vieja, no es contexto válido
        return None

    return LastReading(
        value=float(row.latest_value),
        timestamp=row.latest_timestamp,
    )


def get_last_clean_reading(db: Session, sensor_id: int, max_age_seconds: int = 600) -> Optional[LastReading]:
    """Bug 1.4: Obtiene la última lectura LIMPIA (no spike) del sensor.
    
    Esto evita el efecto rebote donde un spike anterior hace que la siguiente
    lectura normal también parezca spike por la diferencia de valores.
    
    FIX CRÍTICO: Solo retorna lectura si es RECIENTE (dentro de max_age_seconds).
    Sin historial válido → NO existe contexto para evaluar delta spike.
    
    Busca en sensor_readings las últimas lecturas y filtra las que no tienen
    un ml_event DELTA_SPIKE asociado en una ventana de 1 minuto.
    """
    row = db.execute(
        text(
            """
            SELECT TOP 1
                r.id AS reading_id,
                r.value,
                r.timestamp
            FROM dbo.sensor_readings r WITH (NOLOCK)
            WHERE r.sensor_id = :sensor_id
              AND r.timestamp >= DATEADD(SECOND, -:max_age, GETUTCDATE())
              AND NOT EXISTS (
                  SELECT 1 FROM dbo.ml_events e WITH (NOLOCK)
                  WHERE e.sensor_id = r.sensor_id
                    AND e.event_code = 'DELTA_SPIKE'
                    AND e.created_at >= DATEADD(SECOND, -30, r.timestamp)
                    AND e.created_at <= DATEADD(SECOND, 30, r.timestamp)
              )
            ORDER BY r.timestamp DESC
            """
        ),
        {"sensor_id": sensor_id, "max_age": max_age_seconds},
    ).fetchone()

    if not row:
        # Fallback: si no hay lecturas limpias recientes, usar la última lectura normal (que también valida edad)
        return get_last_reading(db, sensor_id, max_age_seconds)

    return LastReading(
        value=float(row.value),
        timestamp=row.timestamp,
        reading_id=int(row.reading_id) if row.reading_id else None,
        is_spike=False,
    )


# Bug 3.5: Umbral mínimo de dt para evaluar slope (evita falsos positivos en batch)
MIN_DT_FOR_SLOPE_SECONDS = 1.0


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
    - Slope absoluto: |delta| / dt (unidades/segundo) - SOLO si dt >= 1s
    - Slope relativo: (|delta|/|last|) / dt (1/segundo) - SOLO si dt >= 1s

    Bug 3.5: Si dt < 1s, NO se evalúan thresholds de slope para evitar
    falsos positivos en lecturas batch o muy cercanas.

    Returns:
        dict con is_spike=True si se detecta spike, None en caso contrario
    """
    # Calcular delta y dt
    delta_abs = abs(current_value - last_reading.value)
    current_ts_utc = _to_utc_aware(current_ts)
    last_ts_utc = _to_utc_aware(last_reading.timestamp)
    dt_seconds = (current_ts_utc - last_ts_utc).total_seconds()

    # Evitar división por cero o dt negativo
    if dt_seconds <= 0:
        dt_seconds = 0.001  # 1ms mínimo para cálculos

    delta_rel = 0.0
    if abs(last_reading.value) > 1e-6:
        delta_rel = abs(delta_abs / last_reading.value)

    # Bug 3.5: Solo calcular slope si dt >= umbral mínimo
    can_evaluate_slope = dt_seconds >= MIN_DT_FOR_SLOPE_SECONDS
    slope_abs = (delta_abs / dt_seconds) if can_evaluate_slope else 0.0
    slope_rel = (delta_rel / dt_seconds) if can_evaluate_slope else 0.0

    triggered = []
    reason_parts = []

    # Siempre evaluar delta absoluto y relativo
    if delta_threshold.abs_delta is not None and delta_abs >= float(delta_threshold.abs_delta):
        triggered.append("abs_delta")
        reason_parts.append(f"delta_abs={delta_abs:.6f} >= {float(delta_threshold.abs_delta):.6f}")

    if delta_threshold.rel_delta is not None and delta_rel >= float(delta_threshold.rel_delta):
        triggered.append("rel_delta")
        reason_parts.append(f"delta_rel={delta_rel:.4%} >= {float(delta_threshold.rel_delta):.4%}")

    # Bug 3.5: Solo evaluar slope si dt >= 1s para evitar falsos positivos
    if can_evaluate_slope:
        if delta_threshold.abs_slope is not None and slope_abs >= float(delta_threshold.abs_slope):
            triggered.append("abs_slope")
            reason_parts.append(f"slope_abs={slope_abs:.6f} >= {float(delta_threshold.abs_slope):.6f}")

        if delta_threshold.rel_slope is not None and slope_rel >= float(delta_threshold.rel_slope):
            triggered.append("rel_slope")
            reason_parts.append(f"slope_rel={slope_rel:.6f} >= {float(delta_threshold.rel_slope):.6f}")

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
        "slope_evaluated": can_evaluate_slope,  # Debug: indica si se evaluó slope
    }


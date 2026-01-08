from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable

from sqlalchemy import text

from iot_ingest_services.common.db import get_engine
from iot_ingest_services.ml.baseline import BaselineConfig, predict_moving_average
from iot_ingest_services.ml.metadata import BASELINE_MOVING_AVERAGE


@dataclass(frozen=True)
class RunnerConfig:
    window: int
    horizon_minutes: int
    dedupe_minutes: int
    sleep_seconds: float
    once: bool


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_watermark(conn, sensor_id: int) -> None:
    # idempotent
    conn.execute(
        text(
            """
            IF NOT EXISTS (SELECT 1 FROM dbo.ml_watermarks WHERE sensor_id = :sensor_id)
            BEGIN
              INSERT INTO dbo.ml_watermarks(sensor_id, last_reading_id, last_processed_at)
              VALUES (:sensor_id, NULL, GETDATE())
            END
            """
        ),
        {"sensor_id": sensor_id},
    )


def _get_last_reading_id(conn, sensor_id: int) -> int | None:
    row = conn.execute(
        text("SELECT last_reading_id FROM dbo.ml_watermarks WHERE sensor_id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row:
        return None
    return int(row[0]) if row[0] is not None else None


def _get_sensor_max_reading_id(conn, sensor_id: int) -> int | None:
    row = conn.execute(
        text("SELECT MAX(id) FROM dbo.sensor_readings WHERE sensor_id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _load_recent_values(conn, sensor_id: int, window: int) -> list[float]:
    # FIX PUNTO 2.3: Eliminar CAST AS float, mantener precisión DECIMAL(15,5)
    rows = conn.execute(
        text(
            """
            SELECT TOP (:limit) [value]
            FROM dbo.sensor_readings
            WHERE sensor_id = :sensor_id
            ORDER BY [timestamp] DESC
            """
        ),
        {"sensor_id": sensor_id, "limit": window},
    ).fetchall()

    # Python Decimal → float mantiene mejor precisión que SQL CAST
    return [float(r[0]) if r[0] is not None else 0.0 for r in rows]


def _get_device_id_for_sensor(conn, sensor_id: int) -> int:
    row = conn.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row:
        raise RuntimeError(f"sensor_id not found: {sensor_id}")
    return int(row[0])


def _get_or_create_active_model_id(conn, sensor_id: int) -> int:
    row = conn.execute(
        text(
            """
            SELECT TOP 1 id
            FROM dbo.ml_models
            WHERE sensor_id = :sensor_id AND is_active = 1
            ORDER BY trained_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if row:
        return int(row[0])

    # baseline bootstrap (dev) usando los metadatos centralizados en iot_ingest_services.ml
    created = conn.execute(
        text(
            """
            INSERT INTO dbo.ml_models (sensor_id, model_name, model_type, version, is_active, trained_at)
            OUTPUT INSERTED.id
            VALUES (:sensor_id, :model_name, :model_type, :version, 1, GETDATE())
            """
        ),
        {
            "sensor_id": sensor_id,
            "model_name": BASELINE_MOVING_AVERAGE.name,
            "model_type": BASELINE_MOVING_AVERAGE.model_type,
            "version": BASELINE_MOVING_AVERAGE.version,
        },
    ).fetchone()

    if not created:
        raise RuntimeError("failed to create ml_models row")

    return int(created[0])


def _insert_prediction(
    conn,
    *,
    model_id: int,
    sensor_id: int,
    predicted_value: float,
    confidence: float,
    target_ts_utc: datetime,
) -> int:
    row = conn.execute(
        text(
            """
            INSERT INTO dbo.predictions (
              model_id, sensor_id, predicted_value, confidence, predicted_at, target_timestamp
            )
            OUTPUT INSERTED.id
            VALUES (
              :model_id, :sensor_id, :predicted_value, :confidence, GETDATE(), :target_timestamp
            )
            """
        ),
        {
            "model_id": model_id,
            "sensor_id": sensor_id,
            "predicted_value": predicted_value,
            "confidence": confidence,
            # SQL Server datetime2 expects tz-naive
            "target_timestamp": target_ts_utc.replace(tzinfo=None),
        },
    ).fetchone()

    if not row:
        raise RuntimeError("failed to insert prediction")

    return int(row[0])


def _should_dedupe_event(conn, *, sensor_id: int, event_code: str, dedupe_minutes: int) -> bool:
    row = conn.execute(
        text(
            """
            SELECT TOP 1 1
            FROM dbo.ml_events
            WHERE sensor_id = :sensor_id
              AND event_code = :event_code
              AND status IN ('active', 'acknowledged')
              AND created_at >= DATEADD(minute, -:mins, GETDATE())
            ORDER BY created_at DESC
            """
        ),
        {"sensor_id": sensor_id, "event_code": event_code, "mins": dedupe_minutes},
    ).fetchone()

    return row is not None


def _eval_pred_threshold_and_create_event(
    conn,
    *,
    sensor_id: int,
    device_id: int,
    prediction_id: int,
    predicted_value: float,
    dedupe_minutes: int,
) -> None:
    # Align with existing threshold semantics.
    thr = conn.execute(
        text(
            """
            SELECT TOP 1
              id, condition_type, threshold_value_min, threshold_value_max, severity, name
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id AND is_active = 1
            ORDER BY id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not thr:
        return

    threshold_id, cond, vmin, vmax, severity, thr_name = thr

    violated = False
    vmin_f = float(vmin) if vmin is not None else None
    vmax_f = float(vmax) if vmax is not None else None

    if cond == "greater_than" and vmin_f is not None and predicted_value > vmin_f:
        violated = True
    elif cond == "less_than" and vmin_f is not None and predicted_value < vmin_f:
        violated = True
    elif cond == "out_of_range" and vmin_f is not None and vmax_f is not None:
        violated = predicted_value < vmin_f or predicted_value > vmax_f
    elif cond == "equal_to" and vmin_f is not None and predicted_value == vmin_f:
        violated = True

    if not violated:
        return

    event_code = "PRED_THRESHOLD_BREACH"
    if _should_dedupe_event(conn, sensor_id=sensor_id, event_code=event_code, dedupe_minutes=dedupe_minutes):
        return

    # Map severity -> event_type
    sev = str(severity)
    if sev == "critical":
        event_type = "critical"
    elif sev == "warning":
        event_type = "warning"
    else:
        event_type = "notice"

    title = f"Predicción viola umbral: {thr_name}"
    message = f"predicted_value={predicted_value} threshold_id={int(threshold_id)}"

    payload = (
        '{'
        f'"threshold_id": {int(threshold_id)}, '
        f'"condition_type": "{cond}", '
        f'"threshold_value_min": {"null" if vmin is None else float(vmin)}, '
        f'"threshold_value_max": {"null" if vmax is None else float(vmax)}, '
        f'"predicted_value": {predicted_value}'
        '}'
    )

    conn.execute(
        text(
            """
            INSERT INTO dbo.ml_events (
              device_id, sensor_id, prediction_id,
              event_type, event_code, title, message,
              status, created_at, payload
            )
            VALUES (
              :device_id, :sensor_id, :prediction_id,
              :event_type, :event_code, :title, :message,
              'active', GETDATE(), :payload
            )
            """
        ),
        {
            "device_id": device_id,
            "sensor_id": sensor_id,
            "prediction_id": prediction_id,
            "event_type": event_type,
            "event_code": event_code,
            "title": title,
            "message": message,
            "payload": payload,
        },
    )


def _update_watermark(conn, *, sensor_id: int, last_reading_id: int) -> None:
    conn.execute(
        text(
            """
            UPDATE dbo.ml_watermarks
            SET last_reading_id = :last_reading_id,
                last_processed_at = GETDATE()
            WHERE sensor_id = :sensor_id
            """
        ),
        {"sensor_id": sensor_id, "last_reading_id": last_reading_id},
    )


def _list_active_sensors(conn) -> Iterable[int]:
    rows = conn.execute(
        text("SELECT id FROM dbo.sensors WHERE is_active = 1 ORDER BY id ASC")
    ).fetchall()
    for r in rows:
        yield int(r[0])


def run_once(cfg: RunnerConfig) -> None:
    engine = get_engine()

    # one transaction per run (you can change to per-sensor if desired)
    with engine.begin() as conn:
        for sensor_id in _list_active_sensors(conn):
            _ensure_watermark(conn, sensor_id)

            last_id = _get_last_reading_id(conn, sensor_id)
            max_id = _get_sensor_max_reading_id(conn, sensor_id)
            if max_id is None:
                continue

            if last_id is not None and max_id <= last_id:
                continue

            values = _load_recent_values(conn, sensor_id, cfg.window)
            if len(values) < 2:
                _update_watermark(conn, sensor_id=sensor_id, last_reading_id=max_id)
                continue

            # Baseline algorithm: moving average over last N (delegado a iot_ingest_services.ml).
            baseline_cfg = BaselineConfig(window=cfg.window)
            predicted_value, confidence = predict_moving_average(values, baseline_cfg)

            model_id = _get_or_create_active_model_id(conn, sensor_id)
            target_ts = _utc_now() + timedelta(minutes=cfg.horizon_minutes)

            prediction_id = _insert_prediction(
                conn,
                model_id=model_id,
                sensor_id=sensor_id,
                predicted_value=predicted_value,
                confidence=confidence,
                target_ts_utc=target_ts,
            )

            device_id = _get_device_id_for_sensor(conn, sensor_id)
            _eval_pred_threshold_and_create_event(
                conn,
                sensor_id=sensor_id,
                device_id=device_id,
                prediction_id=prediction_id,
                predicted_value=predicted_value,
                dedupe_minutes=cfg.dedupe_minutes,
            )

            _update_watermark(conn, sensor_id=sensor_id, last_reading_id=max_id)


def main() -> None:
    p = argparse.ArgumentParser(description="ML batch runner (predictions + ml_events) with watermarks")
    p.add_argument("--window", type=int, default=60)
    p.add_argument("--horizon-minutes", type=int, default=10)
    p.add_argument("--dedupe-minutes", type=int, default=10)
    p.add_argument("--sleep-seconds", type=float, default=60.0)
    p.add_argument("--once", action="store_true", help="run a single iteration and exit")

    args = p.parse_args()

    cfg = RunnerConfig(
        window=args.window,
        horizon_minutes=args.horizon_minutes,
        dedupe_minutes=args.dedupe_minutes,
        sleep_seconds=args.sleep_seconds,
        once=bool(args.once),
    )

    while True:
        run_once(cfg)
        if cfg.once:
            return
        time.sleep(cfg.sleep_seconds)


if __name__ == "__main__":
    main()

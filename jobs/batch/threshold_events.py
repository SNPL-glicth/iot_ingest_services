"""Threshold evaluation and ML event creation for batch runner."""

from __future__ import annotations

import logging

from sqlalchemy import text

logger = logging.getLogger(__name__)


def should_dedupe_event(conn, *, sensor_id: int, event_code: str, dedupe_minutes: int) -> bool:
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


def is_value_within_warning_range(conn, sensor_id: int, value: float) -> bool:
    """Si el valor está dentro de [warning_min, warning_max], ML NO genera eventos."""
    row = conn.execute(
        text(
            """
            SELECT threshold_value_min, threshold_value_max
            FROM dbo.alert_thresholds
            WHERE sensor_id = :sensor_id
              AND is_active = 1
              AND severity = 'warning'
              AND condition_type = 'out_of_range'
            ORDER BY id ASC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return False
    warning_min = float(row[0]) if row[0] is not None else None
    warning_max = float(row[1]) if row[1] is not None else None
    if warning_min is None and warning_max is None:
        return False
    if warning_min is not None and value < warning_min:
        return False
    if warning_max is not None and value > warning_max:
        return False
    return True


def eval_pred_threshold_and_create_event(
    conn, *, sensor_id: int, device_id: int,
    prediction_id: int, predicted_value: float, dedupe_minutes: int,
) -> None:
    if is_value_within_warning_range(conn, sensor_id, predicted_value):
        return

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
    vmin_f = float(vmin) if vmin is not None else None
    vmax_f = float(vmax) if vmax is not None else None

    violated = False
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
    if should_dedupe_event(conn, sensor_id=sensor_id, event_code=event_code, dedupe_minutes=dedupe_minutes):
        return

    sev = str(severity)
    event_type = "critical" if sev == "critical" else ("warning" if sev == "warning" else "notice")
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
            "device_id": device_id, "sensor_id": sensor_id,
            "prediction_id": prediction_id, "event_type": event_type,
            "event_code": event_code, "title": title,
            "message": message, "payload": payload,
        },
    )

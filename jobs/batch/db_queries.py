"""SQL helper functions for the batch runner.

All database queries are centralized here. No business logic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from sqlalchemy import text

from .retry import execute_with_retry


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_watermark(conn, sensor_id: int) -> None:
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


def get_last_reading_id(conn, sensor_id: int) -> int | None:
    row = conn.execute(
        text("SELECT last_reading_id FROM dbo.ml_watermarks WHERE sensor_id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row:
        return None
    return int(row[0]) if row[0] is not None else None


def get_sensor_max_reading_id(conn, sensor_id: int) -> int | None:
    result = execute_with_retry(
        conn,
        "SELECT MAX(id) FROM dbo.sensor_readings WHERE sensor_id = :sensor_id",
        {"sensor_id": sensor_id},
    )
    row = result.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def load_recent_values(conn, sensor_id: int, window: int) -> list[float]:
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
    return [float(r[0]) if r[0] is not None else 0.0 for r in rows]


def load_recent_values_with_timestamps(
    conn, sensor_id: int, window: int,
) -> list[tuple[float, float]]:
    """Load recent values WITH timestamps in chronological order (ASC).

    Returns list of (value, timestamp_epoch) tuples, oldest first.
    Filters out NULL values. Used by enterprise path to avoid double query.
    """
    rows = conn.execute(
        text(
            """
            SELECT TOP (:limit) [value], [timestamp]
            FROM dbo.sensor_readings
            WHERE sensor_id = :sensor_id AND [value] IS NOT NULL
            ORDER BY [timestamp] DESC
            """
        ),
        {"sensor_id": sensor_id, "limit": window},
    ).fetchall()
    result = []
    for r in reversed(rows):
        val = float(r[0]) if r[0] is not None else 0.0
        ts = r[1]
        if isinstance(ts, datetime):
            ts = ts.replace(tzinfo=timezone.utc).timestamp()
        elif ts is not None:
            ts = float(ts)
        else:
            ts = 0.0
        result.append((val, ts))
    return result


def get_device_id_for_sensor(conn, sensor_id: int) -> int:
    row = conn.execute(
        text("SELECT device_id FROM dbo.sensors WHERE id = :sensor_id"),
        {"sensor_id": sensor_id},
    ).fetchone()
    if not row:
        raise RuntimeError(f"sensor_id not found: {sensor_id}")
    return int(row[0])


def get_or_create_active_model_id(conn, sensor_id: int, model_meta) -> int:
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
            "model_name": model_meta.name,
            "model_type": model_meta.model_type,
            "version": model_meta.version,
        },
    ).fetchone()

    if not created:
        raise RuntimeError("failed to create ml_models row")
    return int(created[0])


def update_watermark(conn, *, sensor_id: int, last_reading_id: int) -> None:
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


def load_first_actual_after_timestamp(
    conn, sensor_id: int, after_timestamp: float
) -> float | None:
    """Load the first real sensor reading recorded after a given timestamp.

    Used to provide ground-truth feedback to the plasticity system after a
    prediction has been persisted.  Returns None if no reading exists yet.
    """
    row = conn.execute(
        text(
            """
            SELECT TOP 1 [value]
            FROM dbo.sensor_readings
            WHERE sensor_id = :sensor_id
              AND [timestamp] > :after_ts
              AND [value] IS NOT NULL
            ORDER BY [timestamp] ASC
            """
        ),
        {"sensor_id": sensor_id, "after_ts": after_timestamp},
    ).fetchone()
    if not row or row[0] is None:
        return None
    return float(row[0])


def list_active_sensors(conn) -> list[int]:
    rows = conn.execute(
        text("SELECT id FROM dbo.sensors WHERE is_active = 1 ORDER BY id ASC")
    ).fetchall()
    return [int(r[0]) for r in rows]


def load_all_sensor_data(conn, sensor_ids: list[int], window: int) -> dict[int, list[tuple[float, float]]]:
    """Bulk load recent values with timestamps for multiple sensors.
    
    Returns dict[sensor_id] -> [(value, timestamp), ...] in chronological order.
    """
    if not sensor_ids:
        return {}
    
    placeholders = ','.join(f':sid{i}' for i in range(len(sensor_ids)))
    params = {f'sid{i}': sid for i, sid in enumerate(sensor_ids)}
    params['window'] = window
    
    query = f"""
        SELECT sensor_id, [value], [timestamp]
        FROM (
            SELECT sensor_id, [value], [timestamp],
                   ROW_NUMBER() OVER (PARTITION BY sensor_id ORDER BY [timestamp] DESC) as rn
            FROM dbo.sensor_readings
            WHERE sensor_id IN ({placeholders}) AND [value] IS NOT NULL
        ) ranked
        WHERE rn <= :window
        ORDER BY sensor_id, [timestamp] ASC
    """
    
    rows = conn.execute(text(query), params).fetchall()
    
    result = {}
    for r in rows:
        sid = int(r[0])
        val = float(r[1]) if r[1] is not None else 0.0
        ts = r[2]
        if isinstance(ts, datetime):
            ts = ts.replace(tzinfo=timezone.utc).timestamp()
        elif ts is not None:
            ts = float(ts)
        else:
            ts = 0.0
        
        if sid not in result:
            result[sid] = []
        result[sid].append((val, ts))
    
    return result


def bulk_update_watermarks(conn, watermarks: dict[int, int]) -> None:
    """Bulk update watermarks for multiple sensors.
    
    Args:
        watermarks: dict[sensor_id] -> last_reading_id
    """
    if not watermarks:
        return
    
    values = [
        {"sid": sid, "rid": rid}
        for sid, rid in watermarks.items()
    ]
    
    conn.execute(
        text("""
            UPDATE w
            SET w.last_reading_id = v.rid,
                w.last_processed_at = GETDATE()
            FROM dbo.ml_watermarks w
            INNER JOIN (VALUES :values) AS v(sid, rid)
                ON w.sensor_id = v.sid
        """),
        {"values": values}
    )

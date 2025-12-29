from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from iot_ingest_services.common.db import get_db
from iot_ingest_services.ml_service.reading_broker import Reading, ReadingBroker
from iot_ingest_services.ml_service.in_memory_broker import InMemoryReadingBroker
from .schemas import (
    BulkSensorReadingsIn,
    DevicePacketIn,
    IngestResult,
    PacketIngestResult,
    SensorReadingIn,
)

app = FastAPI(title="IoT Ingest Service", version="0.3.0")


# Broker de lecturas para ML online (MVP: implementación en memoria).
#
# Importante: ingest_api solo publica en el broker; no conoce detalles
# de ML ni ejecuta modelos. ML se suscribe a este broker en otro
# componente/proceso usando la interfaz ReadingBroker.
_broker: ReadingBroker = InMemoryReadingBroker()

# Simple per-process cache.
# Key: (device_uuid, sensor_uuid) -> (sensor_id, expires_at_utc)
_SENSOR_MAP_CACHE: Dict[Tuple[str, str], Tuple[int, datetime]] = {}


def _require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    # If INGEST_API_KEY is not set, we allow requests (dev mode).
    expected = os.getenv("INGEST_API_KEY")
    if not expected:
        return

    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")


@app.get("/health")
def health():
    return {"status": "ok"}


def _cache_ttl_seconds() -> int:
    return int(os.getenv("SENSOR_MAP_TTL_SECONDS", "300"))


def _exec_insert_sp(db: Session, sensor_id: int, value: float) -> None:
    # Keep all threshold/alert logic inside SQL Server.
    # The SP also handles inserting into sensor_readings.
    db.execute(
        text(
            "EXEC sp_insert_reading_and_check_threshold "
            "@p_sensor_id = :sensor_id, @p_value = :value"
        ),
        {"sensor_id": sensor_id, "value": value},
    )

    # Publicar lectura en el broker para consumo por ML online.
    # Aquí no ejecutamos lógica de ML ni interpretamos thresholds;
    # solo enviamos el dato crudo a la capa de inteligencia.
    now_ts = datetime.now(timezone.utc).timestamp()
    reading = Reading(
        sensor_id=sensor_id,
        sensor_type="unknown",  # opcional: resolver desde BD si lo necesitas
        value=float(value),
        timestamp=now_ts,
    )
    _broker.publish(reading)


def _try_exec_bulk_json(db: Session, rows: list[dict]) -> bool:
    # Preferred path: 1 DB call per packet/batch.
    # rows item format: {sensor_id:int, value:float, device_timestamp?:str}
    payload = json.dumps(rows)

    try:
        db.execute(
            text(
                "EXEC sp_insert_readings_bulk_json_and_check_threshold "
                "@p_readings_json = :payload"
            ),
            {"payload": payload},
        )
        return True
    except Exception as e:
        # If the patch wasn't applied yet, fallback to the per-reading SP.
        msg = str(e)
        if "Could not find stored procedure" in msg and "sp_insert_readings_bulk_json_and_check_threshold" in msg:
            return False
        if "sp_insert_readings_bulk_json_and_check_threshold" in msg and "could not" in msg.lower():
            return False
        raise


def _resolve_sensor_id(db: Session, device_uuid: UUID, sensor_uuid: UUID) -> int | None:
    # Cache lookup
    now = datetime.now(timezone.utc)
    key = (str(device_uuid).lower(), str(sensor_uuid).lower())
    cached = _SENSOR_MAP_CACHE.get(key)
    if cached is not None:
        sensor_id, expires_at = cached
        if expires_at > now:
            return sensor_id
        _SENSOR_MAP_CACHE.pop(key, None)

    # Validate sensor belongs to device (prevents spoofing and reduces wrong inserts)
    row = db.execute(
        text(
            "SELECT TOP 1 s.id "
            "FROM sensors s "
            "JOIN devices d ON d.id = s.device_id "
            "WHERE d.device_uuid = :device_uuid AND s.sensor_uuid = :sensor_uuid"
        ),
        {"device_uuid": str(device_uuid), "sensor_uuid": str(sensor_uuid)},
    ).fetchone()

    if not row:
        return None

    sensor_id = int(row[0])
    expires_at = now + timedelta(seconds=_cache_ttl_seconds())
    _SENSOR_MAP_CACHE[key] = (sensor_id, expires_at)
    return sensor_id


# Legacy endpoint (by internal sensor_id). Kept for compatibility/testing.
@app.post("/ingest/readings", response_model=IngestResult, dependencies=[Depends(_require_api_key)])
def ingest_reading(payload: SensorReadingIn, db: Session = Depends(get_db)):
    try:
        _exec_insert_sp(db, payload.sensor_id, float(payload.value))
        db.commit()
        return IngestResult(inserted=1)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")


# Legacy bulk endpoint (by internal sensor_id). Kept for compatibility/testing.
@app.post("/ingest/readings/bulk", response_model=IngestResult, dependencies=[Depends(_require_api_key)])
def ingest_readings_bulk(payload: BulkSensorReadingsIn, db: Session = Depends(get_db)):
    if not payload.readings:
        return IngestResult(inserted=0)

    rows = [{"sensor_id": r.sensor_id, "value": float(r.value)} for r in payload.readings]

    try:
        used_bulk = _try_exec_bulk_json(db, rows)
        if not used_bulk:
            for r in rows:
                _exec_insert_sp(db, int(r["sensor_id"]), float(r["value"]))

        db.commit()
        return IngestResult(inserted=len(rows))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")


# Recommended endpoint: device packet with multiple sensor readings using UUIDs.
@app.post("/ingest/packets", response_model=PacketIngestResult, dependencies=[Depends(_require_api_key)])
def ingest_packet(payload: DevicePacketIn, db: Session = Depends(get_db)):
    if not payload.readings:
        return PacketIngestResult(inserted=0, unknown_sensors=[])

    unknown: list[UUID] = []
    rows: list[dict] = []

    device_ts = payload.ts
    device_ts_iso = device_ts.isoformat() if device_ts is not None else None

    try:
        for r in payload.readings:
            sensor_id = _resolve_sensor_id(db, payload.device_uuid, r.sensor_uuid)
            if sensor_id is None:
                unknown.append(r.sensor_uuid)
                continue

            row = {"sensor_id": sensor_id, "value": float(r.value)}
            if device_ts_iso is not None:
                row["device_timestamp"] = device_ts_iso
            rows.append(row)

        if rows:
            used_bulk = _try_exec_bulk_json(db, rows)
            if not used_bulk:
                for r in rows:
                    _exec_insert_sp(db, int(r["sensor_id"]), float(r["value"]))
            else:
                # Si usamos el SP bulk, publicamos lecturas también en el broker.
                now_ts = datetime.now(timezone.utc).timestamp()
                for r in rows:
                    reading = Reading(
                        sensor_id=int(r["sensor_id"]),
                        sensor_type="unknown",
                        value=float(r["value"]),
                        timestamp=now_ts,
                    )
                    _broker.publish(reading)

        db.commit()
        return PacketIngestResult(inserted=len(rows), unknown_sensors=unknown)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")

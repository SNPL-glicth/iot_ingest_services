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
from iot_ingest_services.ml_service.reading_broker import ReadingBroker
from iot_ingest_services.ml_service.in_memory_broker import InMemoryReadingBroker
from .ingest.router import ReadingRouter
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

# Router central para clasificación y enrutamiento a pipelines
# Se inicializa por request para evitar problemas de estado compartido
def _get_router(db: Session) -> ReadingRouter:
    return ReadingRouter(db, _broker)

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


def _ingest_single_reading(
    db: Session,
    sensor_id: int,
    value: float,
    device_timestamp: datetime | None = None,
) -> None:
    """Ingesta de una sola lectura usando clasificación por propósito.

    Clasifica la lectura ANTES de persistir y la envía al pipeline correspondiente.
    Mantiene compatibilidad con endpoints existentes.
    """
    router = _get_router(db)
    router.classify_and_route(
        sensor_id=sensor_id,
        value=value,
        device_timestamp=device_timestamp,
    )


def _ingest_bulk_readings(
    db: Session,
    rows: list[dict],
) -> None:
    """Ingesta en lote usando clasificación por propósito.

    Procesa cada lectura individualmente con clasificación,
    enrutando a los pipelines correspondientes.

    rows format: {sensor_id:int, value:float, device_timestamp?:datetime}
    """
    router = _get_router(db)

    for row in rows:
        sensor_id = int(row["sensor_id"])
        value = float(row["value"])
        device_ts = row.get("device_timestamp")
        if device_ts and isinstance(device_ts, str):
            # Parse ISO format string if needed
            try:
                device_ts = datetime.fromisoformat(device_ts.replace("Z", "+00:00"))
            except Exception:
                device_ts = None

        # Clasificar y enrutar al pipeline correspondiente
        router.classify_and_route(
            sensor_id=sensor_id,
            value=value,
            device_timestamp=device_ts,
        )


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
    """Endpoint legacy: ingesta de una lectura por sensor_id.

    Usa la nueva arquitectura de clasificación por propósito.
    """
    try:
        device_ts = payload.timestamp
        _ingest_single_reading(
            db=db,
            sensor_id=payload.sensor_id,
            value=float(payload.value),
            device_timestamp=device_ts,
        )
        db.commit()
        return IngestResult(inserted=1)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")


# Legacy bulk endpoint (by internal sensor_id). Kept for compatibility/testing.
@app.post("/ingest/readings/bulk", response_model=IngestResult, dependencies=[Depends(_require_api_key)])
def ingest_readings_bulk(payload: BulkSensorReadingsIn, db: Session = Depends(get_db)):
    """Endpoint legacy: ingesta en lote por sensor_id.

    Usa la nueva arquitectura de clasificación por propósito.
    """
    if not payload.readings:
        return IngestResult(inserted=0)

    rows = [
        {
            "sensor_id": r.sensor_id,
            "value": float(r.value),
            "device_timestamp": r.timestamp,
        }
        for r in payload.readings
    ]

    try:
        _ingest_bulk_readings(db, rows)
        db.commit()
        return IngestResult(inserted=len(rows))
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")


# Recommended endpoint: device packet with multiple sensor readings using UUIDs.
@app.post("/ingest/packets", response_model=PacketIngestResult, dependencies=[Depends(_require_api_key)])
def ingest_packet(payload: DevicePacketIn, db: Session = Depends(get_db)):
    """Endpoint recomendado: ingesta de paquete por dispositivo usando UUIDs.

    Usa la nueva arquitectura de clasificación por propósito:
    - Clasifica cada lectura ANTES de persistir
    - Enruta a flujos separados (alert/warning/prediction)
    - Solo publica datos limpios en el broker ML
    """
    if not payload.readings:
        return PacketIngestResult(inserted=0, unknown_sensors=[])

    unknown: list[UUID] = []
    rows: list[dict] = []

    device_ts = payload.ts

    try:
        for r in payload.readings:
            sensor_id = _resolve_sensor_id(db, payload.device_uuid, r.sensor_uuid)
            if sensor_id is None:
                unknown.append(r.sensor_uuid)
                continue

            row = {"sensor_id": sensor_id, "value": float(r.value)}
            if device_ts is not None:
                row["device_timestamp"] = device_ts
            rows.append(row)

        if rows:
            # Usar la nueva arquitectura de clasificación por propósito
            _ingest_bulk_readings(db, rows)

        db.commit()
        return PacketIngestResult(inserted=len(rows), unknown_sensors=unknown)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}")

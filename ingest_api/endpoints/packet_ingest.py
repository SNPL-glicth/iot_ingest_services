"""Endpoint para ingesta de paquetes por dispositivo (recomendado)."""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from iot_ingest_services.common.db import get_db
from ..auth import require_device_key_dependency, validate_device_access
from ..broker import get_broker
from ..rate_limiter import get_rate_limiter, get_client_ip
from ..schemas import DevicePacketIn, PacketIngestResult
from ..ingest.handlers import BatchReadingHandler
from ..ingest.sensor_resolver import resolve_sensor_id
from ..debug import log_db_identity, should_force_persist, force_persist_probe

router = APIRouter(tags=["ingest"])
logger = logging.getLogger(__name__)


@router.post("/ingest/packets", response_model=PacketIngestResult)
def ingest_packet(
    payload: DevicePacketIn,
    request: Request,
    db: Session = Depends(get_db),
    device_key: str | None = Depends(require_device_key_dependency),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
    """Endpoint recomendado: ingesta de paquete por dispositivo usando UUIDs.

    AUTENTICACIÓN:
    - Nuevo modo: X-Device-Key (API key única por dispositivo)
    - Legacy: X-API-Key (global, si DEVICE_AUTH_ENABLED != 1)

    FLUJO:
    - Rate limiting por IP y dispositivo
    - Valida que la API key pertenece al device_uuid
    - Clasifica cada lectura ANTES de persistir
    - Enruta a flujos separados (alert/warning/prediction)
    - Solo publica datos limpios en el broker ML
    - Actualiza last_seen_at del dispositivo
    """
    if not payload.readings:
        return PacketIngestResult(inserted=0, unknown_sensors=[])

    limiter = get_rate_limiter()
    limiter.check_all(
        device_uuid=str(payload.device_uuid),
        ip=get_client_ip(request),
    )

    validate_device_access(db, str(payload.device_uuid), device_key)

    unknown: list[UUID] = []
    rows: list[dict] = []
    device_ts = payload.ts

    try:
        for r in payload.readings:
            sensor_id = resolve_sensor_id(db, payload.device_uuid, r.sensor_uuid)
            if sensor_id is None:
                unknown.append(r.sensor_uuid)
                continue

            row = {"sensor_id": sensor_id, "value": float(r.value)}
            if device_ts is not None:
                row["device_timestamp"] = device_ts
            rows.append(row)

        if rows:
            broker = get_broker()
            handler = BatchReadingHandler(db, broker)
            handler.ingest(rows)

        log_db_identity(db)
        if should_force_persist(header_value=x_debug_force_persist) and rows:
            first = rows[0]
            force_persist_probe(
                db=db,
                sensor_id=int(first["sensor_id"]),
                ingest_timestamp=datetime.now(timezone.utc),
                device_timestamp=first.get("device_timestamp"),
            )

        db.commit()
        return PacketIngestResult(inserted=len(rows), unknown_sensors=unknown)
    except Exception as e:
        logger.exception("DB error in /ingest/packets err=%s", type(e).__name__)
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)

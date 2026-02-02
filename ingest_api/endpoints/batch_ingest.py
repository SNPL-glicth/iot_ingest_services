"""Endpoint para ingesta en lote (legacy)."""

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from sqlalchemy.orm import Session

from iot_ingest_services.common.db import get_db
from ..auth import require_api_key
from ..broker import get_broker
from ..rate_limiter import get_rate_limiter, get_client_ip
from ..schemas import BulkSensorReadingsIn, IngestResult
from ..pipelines.handlers import BatchReadingHandler
from ..debug import log_db_identity, should_force_persist, force_persist_probe

router = APIRouter(tags=["ingest"])
logger = logging.getLogger(__name__)


@router.post(
    "/ingest/readings/bulk", 
    response_model=IngestResult, 
    dependencies=[Depends(require_api_key)]
)
def ingest_readings_bulk(
    payload: BulkSensorReadingsIn,
    request: Request,
    db: Session = Depends(get_db),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
    """Endpoint legacy: ingesta en lote por sensor_id.

    Usa la arquitectura de clasificación por propósito.
    """
    if not payload.readings:
        return IngestResult(inserted=0)
    
    limiter = get_rate_limiter()
    sensor_ids = list(set(r.sensor_id for r in payload.readings))
    limiter.check_all(
        sensor_ids=sensor_ids,
        ip=get_client_ip(request),
    )

    rows = [
        {
            "sensor_id": r.sensor_id,
            "value": float(r.value),
            "device_timestamp": r.timestamp,
        }
        for r in payload.readings
    ]

    try:
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
        return IngestResult(inserted=len(rows))
    except Exception as e:
        logger.exception("DB error in /ingest/readings/bulk err=%s", type(e).__name__)
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)

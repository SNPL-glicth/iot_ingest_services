"""Endpoint para ingesta de una sola lectura (legacy)."""

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
from ..schemas import SensorReadingIn, IngestResult
from ..pipelines.handlers import SingleReadingHandler
from ..debug import log_db_identity, should_force_persist, force_persist_probe

router = APIRouter(tags=["ingest"])
logger = logging.getLogger(__name__)


@router.post(
    "/ingest/readings", 
    response_model=IngestResult, 
    dependencies=[Depends(require_api_key)]
)
def ingest_reading(
    payload: SensorReadingIn,
    request: Request,
    db: Session = Depends(get_db),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
    """Endpoint legacy: ingesta de una lectura por sensor_id.

    Usa la arquitectura de clasificación por propósito.
    """
    limiter = get_rate_limiter()
    limiter.check_all(
        sensor_ids=[payload.sensor_id],
        ip=get_client_ip(request),
    )
    
    try:
        device_ts = payload.timestamp
        broker = get_broker()
        handler = SingleReadingHandler(db, broker)
        
        handler.ingest(
            sensor_id=payload.sensor_id,
            value=float(payload.value),
            device_timestamp=device_ts,
        )

        log_db_identity(db)
        if should_force_persist(header_value=x_debug_force_persist):
            force_persist_probe(
                db=db,
                sensor_id=int(payload.sensor_id),
                ingest_timestamp=datetime.now(timezone.utc),
                device_timestamp=device_ts,
            )

        db.commit()
        return IngestResult(inserted=1)
    except Exception as e:
        logger.exception("DB error in /ingest/readings err=%s", type(e).__name__)
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)

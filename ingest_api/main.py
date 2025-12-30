from __future__ import annotations

import json
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from iot_ingest_services.common.db import get_db
from iot_ingest_services.ml_service.reading_broker import Reading, ReadingBroker
from iot_ingest_services.ml_service.in_memory_broker import InMemoryReadingBroker
from .ingest.router import ReadingRouter
from .schemas import (
    BulkSensorReadingsIn,
    DevicePacketIn,
    IngestResult,
    PacketIngestResult,
    SensorConsolidatedStatus,
    SensorFinalState,
    ActiveAlert,
    ActiveWarning,
    CurrentPrediction,
    SensorReadingIn,
)

app = FastAPI(title="IoT Ingest Service", version="0.3.0")


def _log_db_identity(db: Session) -> None:
    logger = logging.getLogger(__name__)
    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT
                      DB_NAME() AS db_name,
                      @@SERVERNAME AS server_name,
                      SUSER_SNAME() AS user_name,
                      @@SPID AS spid
                    """
                )
            )
            .mappings()
            .one()
        )
        logger.error(
            "[DB] USING_DB=%s SERVER=%s USER=%s SPID=%s",
            row.get("db_name"),
            row.get("server_name"),
            row.get("user_name"),
            row.get("spid"),
        )
    except Exception as e:
        logger.exception("[DB] Failed to log DB identity err=%s", type(e).__name__)


def _should_force_persist(*, header_value: str | None) -> bool:
    if os.getenv("INGEST_DEBUG_FORCE_PERSIST", "").strip() == "1":
        return True
    if not header_value:
        return False
    v = header_value.strip().lower()
    return v in ("1", "true", "yes", "on")


def _force_persist_probe(
    *,
    db: Session,
    sensor_id: int,
    ingest_timestamp: datetime,
    device_timestamp: datetime | None,
) -> None:
    logger = logging.getLogger(__name__)
    # Inserción de prueba en la MISMA transacción/sesión.
    # Usa el mismo sensor_id del request para evitar FK/validaciones.
    # Valor distintivo para poder localizarlo.
    debug_value = 999999.0
    db.execute(
        text(
            """
            INSERT INTO dbo.sensor_readings (sensor_id, value, timestamp, device_timestamp)
            VALUES (:sensor_id, :value, :ts, :device_ts)
            """
        ),
        {
            "sensor_id": int(sensor_id),
            "value": float(debug_value),
            "ts": ingest_timestamp,
            "device_ts": device_timestamp,
        },
    )

    try:
        row = (
            db.execute(
                text(
                    """
                    SELECT
                      @@TRANCOUNT AS trancount,
                      (
                        SELECT COUNT(*)
                        FROM dbo.sensor_readings
                        WHERE sensor_id = :sensor_id AND value = :value AND timestamp = :ts
                      ) AS seen
                    """
                ),
                {
                    "sensor_id": int(sensor_id),
                    "value": float(debug_value),
                    "ts": ingest_timestamp,
                },
            )
            .mappings()
            .one()
        )
        trancount = row.get("trancount")
        seen = row.get("seen")
    except Exception:
        trancount = None
        seen = None

    logger.error(
        "[DB] FORCE_PERSIST_PROBE inserted dbo.sensor_readings sensor_id=%s value=%s ts=%s trancount=%s seen_in_tx=%s",
        sensor_id,
        debug_value,
        ingest_timestamp.isoformat(),
        trancount,
        seen,
    )


class ThrottledReadingBroker(ReadingBroker):
    def __init__(self, inner: ReadingBroker, *, min_interval_seconds: float) -> None:
        self._inner = inner
        self._min_interval_seconds = float(min_interval_seconds)
        self._last_published_ts_by_sensor: Dict[int, float] = {}

    def publish(self, reading: Reading) -> None:  # type: ignore[override]
        last_ts = self._last_published_ts_by_sensor.get(reading.sensor_id)
        if last_ts is not None and (reading.timestamp - last_ts) < self._min_interval_seconds:
            return

        self._last_published_ts_by_sensor[reading.sensor_id] = float(reading.timestamp)
        self._inner.publish(reading)

    def subscribe(self, handler):  # type: ignore[override]
        self._inner.subscribe(handler)


# Broker de lecturas para ML online (MVP: implementación en memoria).
#
# Importante: ingest_api solo publica en el broker; no conoce detalles
# de ML ni ejecuta modelos. ML se suscribe a este broker en otro
# componente/proceso usando la interfaz ReadingBroker.
_broker: ReadingBroker = ThrottledReadingBroker(
    InMemoryReadingBroker(),
    min_interval_seconds=float(os.getenv("ML_PUBLISH_MIN_INTERVAL_SECONDS", "1.0")),
)

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


def _get_active_alert(db: Session, sensor_id: int) -> ActiveAlert | None:
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, device_id, threshold_id, severity, status, triggered_value, triggered_at
            FROM dbo.alerts
            WHERE sensor_id = :sensor_id
              AND status = 'active'
            ORDER BY triggered_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return ActiveAlert(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        device_id=int(row.device_id),
        threshold_id=int(row.threshold_id),
        severity=str(row.severity),
        status=str(row.status),
        triggered_value=float(row.triggered_value),
        triggered_at=row.triggered_at,
    )


def _get_active_warning(db: Session, sensor_id: int) -> ActiveWarning | None:
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, device_id, event_type, event_code, status, created_at, title, message, payload
            FROM dbo.ml_events
            WHERE sensor_id = :sensor_id
              AND event_code = 'DELTA_SPIKE'
              AND status = 'active'
            ORDER BY created_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    payload = None
    try:
        if row.payload is not None:
            payload = json.loads(row.payload)
    except Exception:
        payload = None

    return ActiveWarning(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        device_id=int(row.device_id),
        event_type=str(row.event_type),
        event_code=str(row.event_code),
        status=str(row.status),
        created_at=row.created_at,
        title=str(row.title) if row.title is not None else None,
        message=str(row.message) if row.message is not None else None,
        payload=payload,
    )


def _get_current_prediction(db: Session, sensor_id: int) -> CurrentPrediction | None:
    row = db.execute(
        text(
            """
            SELECT TOP 1
              id, sensor_id, model_id, predicted_value, confidence, predicted_at, target_timestamp
            FROM dbo.predictions
            WHERE sensor_id = :sensor_id
            ORDER BY predicted_at DESC
            """
        ),
        {"sensor_id": sensor_id},
    ).fetchone()

    if not row:
        return None

    return CurrentPrediction(
        id=int(row.id),
        sensor_id=int(row.sensor_id),
        model_id=int(row.model_id),
        predicted_value=float(row.predicted_value),
        confidence=float(row.confidence),
        predicted_at=row.predicted_at,
        target_timestamp=row.target_timestamp,
    )


def _compute_final_state(
    *,
    alert_active: ActiveAlert | None,
    warning_active: ActiveWarning | None,
    prediction_current: CurrentPrediction | None,
) -> SensorFinalState:
    if alert_active is not None:
        return SensorFinalState.ALERT
    if warning_active is not None:
        return SensorFinalState.WARNING
    if prediction_current is not None:
        return SensorFinalState.PREDICTION
    return SensorFinalState.UNKNOWN


@app.get(
    "/sensors/{sensor_id}/status",
    response_model=SensorConsolidatedStatus,
    dependencies=[Depends(_require_api_key)],
)
def get_sensor_consolidated_status(sensor_id: int, db: Session = Depends(get_db)):
    alert_active = _get_active_alert(db, sensor_id)
    warning_active = _get_active_warning(db, sensor_id)
    prediction_current = _get_current_prediction(db, sensor_id)

    final_state = _compute_final_state(
        alert_active=alert_active,
        warning_active=warning_active,
        prediction_current=prediction_current,
    )

    return SensorConsolidatedStatus(
        sensor_id=int(sensor_id),
        final_state=final_state,
        alert_active=alert_active,
        warning_active=warning_active,
        prediction_current=prediction_current,
    )


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
def ingest_reading(
    payload: SensorReadingIn,
    db: Session = Depends(get_db),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
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

        _log_db_identity(db)
        if _should_force_persist(header_value=x_debug_force_persist):
            _force_persist_probe(
                db=db,
                sensor_id=int(payload.sensor_id),
                ingest_timestamp=datetime.now(timezone.utc),
                device_timestamp=device_ts,
            )

        db.commit()
        return IngestResult(inserted=1)
    except Exception as e:
        logging.getLogger(__name__).exception(
            "DB error in /ingest/readings err=%s",
            type(e).__name__,
        )
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)


# Legacy bulk endpoint (by internal sensor_id). Kept for compatibility/testing.
@app.post("/ingest/readings/bulk", response_model=IngestResult, dependencies=[Depends(_require_api_key)])
def ingest_readings_bulk(
    payload: BulkSensorReadingsIn,
    db: Session = Depends(get_db),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
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

        _log_db_identity(db)
        if _should_force_persist(header_value=x_debug_force_persist) and rows:
            first = rows[0]
            _force_persist_probe(
                db=db,
                sensor_id=int(first["sensor_id"]),
                ingest_timestamp=datetime.now(timezone.utc),
                device_timestamp=first.get("device_timestamp"),
            )

        db.commit()
        return IngestResult(inserted=len(rows))
    except Exception as e:
        logging.getLogger(__name__).exception(
            "DB error in /ingest/readings/bulk err=%s",
            type(e).__name__,
        )
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)


# Recommended endpoint: device packet with multiple sensor readings using UUIDs.
@app.post("/ingest/packets", response_model=PacketIngestResult, dependencies=[Depends(_require_api_key)])
def ingest_packet(
    payload: DevicePacketIn,
    db: Session = Depends(get_db),
    x_debug_force_persist: str | None = Header(default=None, alias="X-Debug-Force-Persist"),
):
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

        _log_db_identity(db)
        if _should_force_persist(header_value=x_debug_force_persist) and rows:
            first = rows[0]
            _force_persist_probe(
                db=db,
                sensor_id=int(first["sensor_id"]),
                ingest_timestamp=datetime.now(timezone.utc),
                device_timestamp=first.get("device_timestamp"),
            )

        db.commit()
        return PacketIngestResult(inserted=len(rows), unknown_sensors=unknown)
    except Exception as e:
        logging.getLogger(__name__).exception(
            "DB error in /ingest/packets err=%s",
            type(e).__name__,
        )
        db.rollback()
        detail = f"DB error: {type(e).__name__}"
        if os.getenv("INGEST_DEBUG_ERRORS", "").strip() == "1":
            detail = f"{detail}: {e}"
        raise HTTPException(status_code=500, detail=detail)

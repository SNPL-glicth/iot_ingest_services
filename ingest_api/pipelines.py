"""Pipelines de ingesta separados por propósito.

Este módulo define los 3 pipelines independientes:
- ALERT_PIPELINE: valores fuera de rango físico (severity=critical)
- WARNING_PIPELINE: delta spikes o cambios bruscos
- PREDICTION_PIPELINE: datos limpios para ML (con decimales completos)

Reglas estrictas:
- ALERT y WARNING NO envían datos al ML
- PREDICTION solo recibe datos sin violaciones físicas ni spikes
- Solo 1 alerta / advertencia activa por sensor
- No guardar todas las lecturas, solo eventos relevantes
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy.orm import Session

from sqlalchemy import text

from iot_machine_learning.ml_service.reading_broker import Reading, ReadingBroker
from .classification import ReadingClassifier, ReadingClass, ClassifiedReading
from .ingest_flows import ingest_alert, ingest_warning, ingest_prediction
from .ingest.predictions.prediction_dispatch import should_skip_prediction


def get_sensor_type(db: Session, sensor_id: int) -> str:
    """INC-09: Obtiene el tipo de sensor desde la BD.
    
    Retorna 'unknown' si no se encuentra el sensor.
    """
    try:
        row = db.execute(
            text("SELECT sensor_type FROM dbo.sensors WHERE id = :sensor_id"),
            {"sensor_id": sensor_id},
        ).fetchone()
        if row and row.sensor_type:
            return str(row.sensor_type)
    except Exception:
        pass
    return "unknown"


def classify_reading(
    db: Session,
    sensor_id: int,
    value: float,
    device_timestamp: Optional[datetime] = None,
    ingest_timestamp: Optional[datetime] = None,
) -> ClassifiedReading:
    """Función central de clasificación de lecturas.

    Clasifica una lectura según su propósito ANTES de persistir:
    - ALERT: Violación de rango físico
    - WARNING: Delta spike detectado
    - PREDICTION: Dato limpio para ML

    Args:
        db: Sesión de base de datos
        sensor_id: ID del sensor
        value: Valor de la lectura
        device_timestamp: Timestamp del dispositivo (opcional)
        ingest_timestamp: Timestamp de ingesta (opcional, default: ahora)

    Returns:
        ClassifiedReading con la clasificación y metadata
    """
    if ingest_timestamp is None:
        ingest_timestamp = datetime.now(timezone.utc)

    classifier = ReadingClassifier(db)
    return classifier.classify(
        sensor_id=sensor_id,
        value=value,
        device_timestamp=device_timestamp,
        ingest_timestamp=ingest_timestamp,
    )


def handle_alert_pipeline(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
) -> None:
    """Handler del pipeline de ALERTAS.

    Procesa lecturas que violan el rango físico del sensor.
    - Severity siempre = critical
    - Solo 1 alerta activa por sensor
    - NO envía datos al ML
    """
    ingest_alert(db, classified, ingest_timestamp)
    # ALERT_PIPELINE NO publica en el broker ML


def handle_warning_pipeline(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
) -> None:
    """Handler del pipeline de ADVERTENCIAS.

    Procesa lecturas con delta spikes o cambios bruscos.
    - Solo 1 advertencia activa por sensor
    - NO envía datos al ML
    """
    ingest_warning(db, classified, ingest_timestamp)
    # WARNING_PIPELINE NO publica en el broker ML


def handle_prediction_pipeline(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
    broker: ReadingBroker,
    sensor_id: int,
    value: float,
) -> None:
    """Handler del pipeline de PREDICCIÓN.

    Procesa datos limpios para ML (sin violaciones físicas ni spikes).
    - Conserva decimales completos
    - Publica en el broker ML para procesamiento online
    - NO guarda todas las lecturas masivamente
    """
    logger = logging.getLogger(__name__)

    # Regla PREDICTION (dedupe): si es igual al latest limpio, NO actualizar latest ni publicar a ML.
    skip, reason = should_skip_prediction(db, sensor_id=sensor_id, value=value)
    if skip:
        logger.info(
            "INGEST SKIP PREDICTION sensor_id=%s value=%s ts=%s reason=%s",
            sensor_id,
            value,
            ingest_timestamp.isoformat(),
            reason,
        )
        return

    ingest_prediction(db, classified, ingest_timestamp)
    logger.info(
        "INGEST PERSIST PREDICTION sensor_id=%s value=%s ts=%s reason=%s",
        sensor_id,
        value,
        ingest_timestamp.isoformat(),
        reason,
    )

    # PREDICTION_PIPELINE: Solo datos limpios van al ML
    # INC-09: Resolver sensor_type desde BD en lugar de hardcoded "unknown"
    sensor_type = get_sensor_type(db, sensor_id)
    now_ts = ingest_timestamp.timestamp()
    reading = Reading(
        sensor_id=sensor_id,
        sensor_type=sensor_type,
        value=float(value),
        timestamp=now_ts,
    )
    broker.publish(reading)


def route_reading_to_pipeline(
    db: Session,
    classified: ClassifiedReading,
    ingest_timestamp: datetime,
    broker: ReadingBroker,
) -> None:
    """Enruta una lectura clasificada al pipeline correspondiente.

    Esta es la función central que decide a qué pipeline va cada lectura.
    Mantiene las reglas estrictas:
    - ALERT y WARNING NO envían datos al ML
    - PREDICTION solo recibe datos limpios
    """
    if classified.classification == ReadingClass.ALERT:
        handle_alert_pipeline(db, classified, ingest_timestamp)
    elif classified.classification == ReadingClass.WARNING:
        handle_warning_pipeline(db, classified, ingest_timestamp)
    elif classified.classification == ReadingClass.ML_PREDICTION:
        handle_prediction_pipeline(
            db=db,
            classified=classified,
            ingest_timestamp=ingest_timestamp,
            broker=broker,
            sensor_id=classified.sensor_id,
            value=classified.value,
        )


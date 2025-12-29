"""Validaciones comunes para los pipelines."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from .physical_ranges import PhysicalRange, get_physical_range
from .delta_utils import DeltaThreshold, LastReading, get_delta_threshold, get_last_reading, check_delta_spike


def validate_alert_data(
    db: Session,
    sensor_id: int,
    value: float,
) -> tuple[bool, Optional[PhysicalRange], str]:
    """Valida que los datos pertenezcan al pipeline de ALERTAS.

    Returns:
        (is_valid, physical_range, reason)
    """
    physical_range = get_physical_range(db, sensor_id)
    if not physical_range:
        return False, None, "No hay rango físico configurado para este sensor"

    if not physical_range.violates(value):
        return False, None, f"Valor {value} no viola el rango físico [{physical_range.min_value}, {physical_range.max_value}]"

    return True, physical_range, f"Valor {value} fuera de rango físico [{physical_range.min_value}, {physical_range.max_value}]"


def validate_warning_data(
    db: Session,
    sensor_id: int,
    value: float,
    current_ts: Optional[datetime] = None,
) -> tuple[bool, Optional[dict], str]:
    """Valida que los datos pertenezcan al pipeline de WARNINGS.

    Returns:
        (is_valid, delta_info, reason)
    """
    from datetime import datetime, timezone

    if current_ts is None:
        current_ts = datetime.now(timezone.utc)

    last_reading = get_last_reading(db, sensor_id)
    if not last_reading:
        return False, None, "No hay lectura previa para comparar delta"

    delta_threshold = get_delta_threshold(db, sensor_id)
    if not delta_threshold:
        return False, None, "No hay umbrales de delta configurados para este sensor"

    delta_info = check_delta_spike(
        current_value=value,
        current_ts=current_ts,
        last_reading=last_reading,
        delta_threshold=delta_threshold,
    )

    if not delta_info or not delta_info.get("is_spike", False):
        return False, None, "No se detectó delta spike significativo"

    return True, delta_info, f"Delta spike detectado: {delta_info.get('reason', '')}"


def validate_prediction_data(
    db: Session,
    sensor_id: int,
    value: float,
    current_ts: Optional[datetime] = None,
) -> tuple[bool, str]:
    """Valida que los datos pertenezcan al pipeline de PREDICCIONES.

    Los datos de predicción deben estar limpios:
    - Sin violación física
    - Sin delta spike reciente

    Returns:
        (is_valid, reason)
    """
    from datetime import datetime, timezone

    # Verificar que NO haya violación física
    physical_range = get_physical_range(db, sensor_id)
    if physical_range and physical_range.violates(value):
        return False, f"Valor {value} viola rango físico - debe ir a ALERT pipeline"

    # Verificar que NO haya delta spike
    if current_ts is None:
        current_ts = datetime.now(timezone.utc)

    last_reading = get_last_reading(db, sensor_id)
    if last_reading:
        delta_threshold = get_delta_threshold(db, sensor_id)
        if delta_threshold:
            delta_info = check_delta_spike(
                current_value=value,
                current_ts=current_ts,
                last_reading=last_reading,
                delta_threshold=delta_threshold,
            )
            if delta_info and delta_info.get("is_spike", False):
                return False, f"Delta spike detectado - debe ir a WARNING pipeline"

    return True, "Dato limpio, apto para ML"


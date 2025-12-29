"""Reglas de negocio para el pipeline de PREDICCIONES."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy.orm import Session

from ..common.validation import validate_prediction_data


class PredictionRules:
    """Reglas y validaciones para el pipeline de PREDICCIONES."""

    @staticmethod
    def accepts(
        db: Session,
        sensor_id: int,
        value: float,
        current_ts: datetime | None = None,
    ) -> tuple[bool, str]:
        """Verifica si los datos deben ser procesados por el pipeline de PREDICCIONES.

        Regla: Solo acepta datos limpios (sin violación física, sin delta spike).

        Returns:
            (should_accept, reason)
        """
        from datetime import datetime, timezone
        if current_ts is None:
            current_ts = datetime.now(timezone.utc)
        return validate_prediction_data(db, sensor_id, value, current_ts)

    @staticmethod
    def should_persist_reading() -> bool:
        """Indica si se debe persistir la lectura completa.

        Regla: NO guardar todas las lecturas masivamente.
        Solo mantener sensor_readings_latest actualizado.
        """
        return False

    @staticmethod
    def should_update_latest() -> bool:
        """Indica si se debe actualizar sensor_readings_latest.

        Regla: Siempre mantener el último valor actualizado.
        """
        return True

    @staticmethod
    def should_forward_to_ml() -> bool:
        """Indica si se debe enviar al broker ML.

        Regla: PREDICTION pipeline SIEMPRE envía datos limpios al ML.
        """
        return True

    @staticmethod
    def preserve_decimals() -> bool:
        """Indica si se deben conservar decimales completos.

        Regla: PREDICTION pipeline conserva precisión completa para ML.
        """
        return True


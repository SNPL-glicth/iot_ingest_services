"""Reglas de negocio para el pipeline de ALERTAS."""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from ..shared.physical_ranges import PhysicalRange
from ..shared.validation import validate_alert_data


class AlertRules:
    """Reglas y validaciones para el pipeline de ALERTAS."""

    @staticmethod
    def accepts(
        db: Session,
        sensor_id: int,
        value: float,
    ) -> tuple[bool, Optional[PhysicalRange], str]:
        """Verifica si los datos deben ser procesados por el pipeline de ALERTAS.

        Regla: Solo acepta valores que violan el rango físico del sensor.

        Returns:
            (should_accept, physical_range, reason)
        """
        return validate_alert_data(db, sensor_id, value)

    @staticmethod
    def get_severity() -> str:
        """Retorna la severidad para alertas físicas.

        Regla estricta: Siempre CRITICAL, nunca puede ser downgradeado.
        """
        return "critical"

    @staticmethod
    def should_persist_reading() -> bool:
        """Indica si se debe persistir la lectura completa.

        Regla: Solo persistir la lectura que rompe el umbral.
        """
        return True

    @staticmethod
    def should_update_latest() -> bool:
        """Indica si se debe actualizar sensor_readings_latest.

        Regla: Siempre mantener el último valor actualizado.
        """
        return True

    @staticmethod
    def should_forward_to_ml() -> bool:
        """Indica si se debe enviar al broker ML.

        Regla estricta: ALERT pipeline NUNCA envía datos al ML.
        """
        return False


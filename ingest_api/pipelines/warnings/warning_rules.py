"""Reglas de negocio para el pipeline de WARNINGS."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from ..shared.validation import validate_warning_data


class WarningRules:
    """Reglas y validaciones para el pipeline de WARNINGS."""

    @staticmethod
    def accepts(
        db: Session,
        sensor_id: int,
        value: float,
        current_ts: Optional[datetime] = None,
    ) -> tuple[bool, Optional[dict], str]:
        """Verifica si los datos deben ser procesados por el pipeline de WARNINGS.

        Regla: Solo acepta valores con delta spike detectado.

        Returns:
            (should_accept, delta_info, reason)
        """
        from datetime import datetime, timezone
        if current_ts is None:
            current_ts = datetime.now(timezone.utc)
        return validate_warning_data(db, sensor_id, value, current_ts)

    @staticmethod
    def get_severity(delta_info: dict | None) -> str:
        """Retorna la severidad para advertencias de delta.

        Regla: Severity basada en la configuración del delta threshold.
        """
        if delta_info:
            severity = delta_info.get("severity", "warning")
            if severity in ("info", "warning", "critical"):
                return severity
        return "warning"

    @staticmethod
    def should_persist_reading() -> bool:
        """Indica si se debe persistir la lectura completa.

        Regla: Solo persistir el evento de delta spike.
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

        Regla estricta: WARNING pipeline NUNCA envía datos al ML.
        """
        return False


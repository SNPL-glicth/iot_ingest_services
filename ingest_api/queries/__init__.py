"""Módulo de queries para consultas a BD.

Contiene funciones puras de consulta sin lógica de negocio.
"""

from .sensor_status import (
    get_active_alert,
    get_active_warning,
    get_current_prediction,
    compute_final_state,
)

__all__ = [
    "get_active_alert",
    "get_active_warning", 
    "get_current_prediction",
    "compute_final_state",
]

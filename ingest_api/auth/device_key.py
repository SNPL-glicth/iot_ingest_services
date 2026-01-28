"""Autenticación por Device Key para endpoints de dispositivos.

Re-exporta las funciones de device_auth.py para mantener compatibilidad.
"""

from ..device_auth import (
    require_device_key_dependency,
    validate_device_access,
)

__all__ = [
    "require_device_key_dependency",
    "validate_device_access",
]

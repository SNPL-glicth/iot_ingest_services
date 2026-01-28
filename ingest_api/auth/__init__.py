"""Módulo de autenticación para endpoints de ingesta.

Consolida toda la lógica de autenticación:
- API Key (legacy endpoints)
- Device Key (endpoints recomendados)
"""

from .api_key import require_api_key
from .device_key import require_device_key_dependency, validate_device_access

__all__ = [
    "require_api_key",
    "require_device_key_dependency",
    "validate_device_access",
]

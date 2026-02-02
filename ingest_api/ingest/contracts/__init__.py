"""Contratos de datos unificados para ingesta.

Define DTOs consistentes para todos los canales de entrada.
"""

from .unified_reading import UnifiedReading, ReadingSource

__all__ = [
    "UnifiedReading",
    "ReadingSource",
]

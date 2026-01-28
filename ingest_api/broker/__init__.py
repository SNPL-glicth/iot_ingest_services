"""Módulo de broker para publicación de lecturas a ML.

El broker transporta lecturas clasificadas como "limpias" al servicio de ML.
NO persiste datos, solo notifica.
"""

from .throttled import ThrottledReadingBroker
from .factory import create_broker, get_broker

__all__ = [
    "ThrottledReadingBroker",
    "create_broker",
    "get_broker",
]

"""Handlers de ingesta separados por tipo.

- single: Ingesta de una sola lectura
- batch: Ingesta en lote (usa BatchInserter)
"""

from .single import SingleReadingHandler
from .batch import BatchReadingHandler

__all__ = [
    "SingleReadingHandler",
    "BatchReadingHandler",
]

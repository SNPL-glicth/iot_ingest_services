"""Endpoint de health check para componentes de resiliencia."""

from __future__ import annotations

from fastapi import APIRouter

from ..ingest.resilience import get_resilience_health

router = APIRouter(tags=["health"])


@router.get("/health/resilience")
def resilience_health():
    """Retorna el estado de salud de los componentes de resiliencia.
    
    Returns:
        Diccionario con estado de Redis, deduplicador y DLQ.
    """
    return get_resilience_health()

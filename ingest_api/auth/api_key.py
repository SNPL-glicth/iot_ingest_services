"""Autenticación por API Key para endpoints legacy.

SECURITY: En producción, INGEST_API_KEY debe estar configurado.
"""

from __future__ import annotations

import os
import logging

from fastapi import Header, HTTPException


def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key")
) -> None:
    """Valida API key para endpoints legacy.
    
    SECURITY FIX: En producción, INGEST_API_KEY debe estar configurado.
    En modo desarrollo, permite acceso sin autenticación con warning.
    """
    logger = logging.getLogger(__name__)
    expected = os.getenv("INGEST_API_KEY")
    is_production = (
        os.getenv("NODE_ENV") == "production" or 
        os.getenv("ENVIRONMENT") == "production"
    )
    
    if not expected:
        if is_production:
            logger.error("CRITICAL: INGEST_API_KEY not configured in production!")
            raise HTTPException(
                status_code=500, 
                detail="Server misconfiguration: API key not set"
            )
        logger.warning(
            "[SECURITY WARNING] INGEST_API_KEY not set - "
            "allowing unauthenticated access (DEV ONLY)"
        )
        return

    if not x_api_key:
        raise HTTPException(status_code=401, detail="API key required")
    
    if x_api_key != expected:
        logger.warning("Invalid API key attempt from request")
        raise HTTPException(status_code=401, detail="Invalid API key")

"""API Key Validator - Validación y autorización de API keys (ISO 27001).

Valida API keys contra PostgreSQL y verifica permisos.
ISO 27001 A.9.4.2 - Secure log-on procedures
"""

from __future__ import annotations

import hashlib
import logging
from typing import Optional

from fastapi import Header, HTTPException, status
from sqlalchemy import text
from sqlalchemy.engine import Engine

from .authorization import ApiKeyInfo, Role
from ..infrastructure.persistence.postgres import get_postgres_engine

logger = logging.getLogger(__name__)


def hash_api_key(api_key: str) -> str:
    """Hashea un API key con SHA-256.
    
    Args:
        api_key: API key en texto plano
        
    Returns:
        Hash SHA-256 del key
    """
    return hashlib.sha256(api_key.encode()).hexdigest()


def validate_api_key_from_db(
    api_key: str,
    engine: Optional[Engine] = None,
) -> ApiKeyInfo:
    """Valida API key contra base de datos PostgreSQL.
    
    Args:
        api_key: API key en texto plano
        engine: Engine de PostgreSQL (opcional)
        
    Returns:
        ApiKeyInfo con permisos del key
        
    Raises:
        HTTPException: 401 si key no existe o inactivo, 500 si error de DB
    """
    if engine is None:
        engine = get_postgres_engine()
    
    if engine is None:
        logger.error("[Auth] PostgreSQL not configured - cannot validate API keys")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication service unavailable",
        )
    
    # Hash del key para comparación segura
    key_hash = hash_api_key(api_key)
    key_prefix = api_key[:8] if len(api_key) >= 8 else api_key
    
    try:
        with engine.begin() as conn:
            result = conn.execute(
                text("""
                    SELECT 
                        id::text as key_id,
                        key_prefix,
                        role,
                        allowed_source_id,
                        allowed_domains,
                        is_active
                    FROM api_keys
                    WHERE key_hash = :key_hash
                    LIMIT 1
                """),
                {"key_hash": key_hash}
            ).fetchone()
            
            if result is None:
                # Key no existe - loguear intento fallido
                logger.warning(
                    "[Auth] Invalid API key attempt - prefix=%s",
                    key_prefix,
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid API key",
                )
            
            # Verificar si está activo
            if not result.is_active:
                logger.warning(
                    "[Auth] Inactive API key attempt - key_id=%s prefix=%s",
                    result.key_id,
                    result.key_prefix,
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="API key is inactive",
                )
            
            # Actualizar last_used_at
            conn.execute(
                text("""
                    UPDATE api_keys
                    SET last_used_at = NOW()
                    WHERE id = :key_id::uuid
                """),
                {"key_id": result.key_id}
            )
            
            # Construir ApiKeyInfo
            api_key_info = ApiKeyInfo(
                key_id=result.key_id,
                key_prefix=result.key_prefix,
                role=Role(result.role),
                allowed_source_id=result.allowed_source_id,
                allowed_domains=result.allowed_domains or [],
                is_active=result.is_active,
            )
            
            logger.info(
                "[Auth] API key validated - key_id=%s role=%s",
                api_key_info.key_id,
                api_key_info.role.value,
            )
            
            return api_key_info
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[Auth] Error validating API key: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Authentication error",
        )


def verify_api_key(
    x_api_key: str = Header(..., alias="X-API-Key")
) -> ApiKeyInfo:
    """Dependency para validar API key en endpoints.
    
    Args:
        x_api_key: API key desde header X-API-Key
        
    Returns:
        ApiKeyInfo con permisos validados
        
    Raises:
        HTTPException: 401 si key inválido o inactivo
    """
    return validate_api_key_from_db(x_api_key)


def verify_source_access(
    api_key_info: ApiKeyInfo,
    source_id: str,
    domain: str,
) -> None:
    """Verifica que el API key tenga acceso al source_id y domain.
    
    Args:
        api_key_info: Información del API key validado
        source_id: Source ID a verificar
        domain: Dominio a verificar
        
    Raises:
        HTTPException: 403 si no tiene permiso
    """
    # Verificar permiso de source_id
    if not api_key_info.can_write_to_source(source_id):
        logger.warning(
            "[Auth] Forbidden source_id access - key_id=%s source_id=%s allowed=%s",
            api_key_info.key_id,
            source_id,
            api_key_info.allowed_source_id,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not authorized to write to source_id '{source_id}'",
        )
    
    # Verificar permiso de domain
    if not api_key_info.can_write_to_domain(domain):
        logger.warning(
            "[Auth] Forbidden domain access - key_id=%s domain=%s allowed=%s",
            api_key_info.key_id,
            domain,
            api_key_info.allowed_domains,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not authorized to write to domain '{domain}'",
        )

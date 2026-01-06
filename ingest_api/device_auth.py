"""
Autenticación de dispositivos IoT por API Key única.

FLUJO:
1. El dispositivo envía X-Device-Key en el header
2. Validamos que el hash de la key existe en device_api_keys
3. Validamos que la key pertenece al device_uuid del body
4. Actualizamos last_used_at y last_seen_at

SEGURIDAD:
- Cada dispositivo tiene su propia API key
- La key se guarda hasheada (SHA256)
- Si una key es comprometida, solo ese dispositivo es afectado
"""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from typing import Optional, Tuple

from fastapi import Header, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session


logger = logging.getLogger(__name__)


def _hash_api_key(api_key: str) -> str:
    """Genera el hash SHA256 de una API key."""
    return hashlib.sha256(api_key.encode()).hexdigest()


def _is_device_auth_enabled() -> bool:
    """Verifica si la autenticación por dispositivo está habilitada."""
    # Si DEVICE_AUTH_ENABLED=1, usar autenticación por dispositivo
    # Si no, mantener compatibilidad con el modo legacy (X-API-Key global)
    return os.getenv("DEVICE_AUTH_ENABLED", "").strip() == "1"


def _is_legacy_mode_allowed() -> bool:
    """Verifica si el modo legacy (X-API-Key global) está permitido."""
    # Por defecto, permitido para compatibilidad hacia atrás
    return os.getenv("LEGACY_API_KEY_ALLOWED", "1").strip() == "1"


def validate_device_api_key(
    db: Session,
    device_uuid: str,
    api_key: str,
) -> Tuple[bool, Optional[int], Optional[str]]:
    """
    Valida que la API key pertenece al dispositivo indicado.
    
    Returns:
        (is_valid, device_id, error_code)
        - is_valid: True si la validación fue exitosa
        - device_id: ID interno del dispositivo si es válido
        - error_code: Código de error si no es válido
    """
    api_key_hash = _hash_api_key(api_key)
    
    try:
        row = db.execute(
            text(
                """
                SELECT 
                    d.id AS device_id,
                    dak.device_id AS key_device_id,
                    dak.is_active,
                    dak.revoked_at,
                    dak.expires_at
                FROM device_api_keys dak
                JOIN devices d ON d.id = dak.device_id
                WHERE dak.api_key_hash = :api_key_hash
                  AND d.device_uuid = :device_uuid
                """
            ),
            {"api_key_hash": api_key_hash, "device_uuid": device_uuid},
        ).fetchone()
        
        if not row:
            # Verificar si el device existe
            device_row = db.execute(
                text("SELECT id FROM devices WHERE device_uuid = :device_uuid"),
                {"device_uuid": device_uuid},
            ).fetchone()
            
            if not device_row:
                return (False, None, "DEVICE_NOT_FOUND")
            
            # Device existe pero la key no coincide
            return (False, None, "INVALID_API_KEY")
        
        # Verificar estado de la key
        if not row.is_active:
            return (False, None, "KEY_INACTIVE")
        
        if row.revoked_at is not None:
            return (False, None, "KEY_REVOKED")
        
        if row.expires_at is not None and row.expires_at < datetime.now(timezone.utc):
            return (False, None, "KEY_EXPIRED")
        
        device_id = int(row.device_id)
        
        # Actualizar last_used_at y last_seen_at
        _update_timestamps(db, device_id, api_key_hash)
        
        return (True, device_id, None)
        
    except Exception as e:
        logger.exception("Error validating device API key: %s", e)
        return (False, None, "VALIDATION_ERROR")


def _update_timestamps(db: Session, device_id: int, api_key_hash: str) -> None:
    """Actualiza last_used_at de la key y last_seen_at del device."""
    try:
        now = datetime.now(timezone.utc)
        
        # Actualizar last_used_at de la API key
        db.execute(
            text(
                """
                UPDATE device_api_keys 
                SET last_used_at = :now 
                WHERE api_key_hash = :api_key_hash
                """
            ),
            {"now": now, "api_key_hash": api_key_hash},
        )
        
        # Actualizar last_seen_at del device
        db.execute(
            text(
                """
                UPDATE devices 
                SET last_seen_at = :now, status = 'online'
                WHERE id = :device_id
                """
            ),
            {"now": now, "device_id": device_id},
        )
        
    except Exception as e:
        # No fallar la ingesta por error de actualización de timestamps
        logger.warning("Failed to update timestamps for device %s: %s", device_id, e)


def require_device_key_dependency(
    x_device_key: str | None = Header(default=None, alias="X-Device-Key"),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
) -> str | None:
    """
    Dependency de FastAPI para requerir autenticación.
    
    Soporta dos modos:
    1. Nuevo: X-Device-Key (autenticación por dispositivo)
    2. Legacy: X-API-Key (autenticación global, si está permitido)
    
    Retorna la API key que debe validarse contra el device_uuid del body.
    """
    if _is_device_auth_enabled():
        # Modo nuevo: requerir X-Device-Key
        if not x_device_key:
            if _is_legacy_mode_allowed() and x_api_key:
                # Fallback a legacy si está permitido
                return None  # Indica que debe validarse con legacy
            raise HTTPException(
                status_code=401, 
                detail="Missing X-Device-Key header"
            )
        return x_device_key
    
    # Modo legacy: usar X-API-Key global
    expected = os.getenv("INGEST_API_KEY")
    if not expected:
        return None  # Dev mode: permite todo
    
    if x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    return None  # Indica que ya está validado con legacy


def validate_device_access(
    db: Session,
    device_uuid: str,
    device_key: str | None,
) -> Optional[int]:
    """
    Valida el acceso de un dispositivo.
    
    Args:
        db: Sesión de base de datos
        device_uuid: UUID del dispositivo
        device_key: API key del dispositivo (None si modo legacy)
    
    Returns:
        device_id si es válido, None si debe continuar con legacy
        
    Raises:
        HTTPException si la validación falla
    """
    if device_key is None:
        # Modo legacy o dev mode
        return None
    
    is_valid, device_id, error_code = validate_device_api_key(
        db, device_uuid, device_key
    )
    
    if not is_valid:
        error_messages = {
            "DEVICE_NOT_FOUND": f"Device {device_uuid} not found",
            "INVALID_API_KEY": "Invalid device API key",
            "KEY_INACTIVE": "Device API key is inactive",
            "KEY_REVOKED": "Device API key has been revoked",
            "KEY_EXPIRED": "Device API key has expired",
            "VALIDATION_ERROR": "Error validating device API key",
        }
        detail = error_messages.get(error_code, "Authentication failed")
        raise HTTPException(status_code=401, detail=detail)
    
    return device_id

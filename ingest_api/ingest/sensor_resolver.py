"""Resolución de sensor_id a partir de UUIDs.

Mantiene un caché en memoria para hot paths O(1).
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session


_SENSOR_MAP_CACHE: Dict[Tuple[str, str], Tuple[int, datetime]] = {}


def _cache_ttl_seconds() -> int:
    """TTL del caché en segundos."""
    return int(os.getenv("SENSOR_MAP_TTL_SECONDS", "300"))


def resolve_sensor_id(
    db: Session, 
    device_uuid: UUID, 
    sensor_uuid: UUID
) -> Optional[int]:
    """Resuelve el sensor_id a partir de UUIDs de dispositivo y sensor.
    
    Usa caché en memoria con TTL para evitar queries repetidas.
    Valida que el sensor pertenece al dispositivo (previene spoofing).
    
    Args:
        db: Sesión de BD
        device_uuid: UUID del dispositivo
        sensor_uuid: UUID del sensor
        
    Returns:
        sensor_id si existe, None si no se encuentra
    """
    now = datetime.now(timezone.utc)
    key = (str(device_uuid).lower(), str(sensor_uuid).lower())
    
    cached = _SENSOR_MAP_CACHE.get(key)
    if cached is not None:
        sensor_id, expires_at = cached
        if expires_at > now:
            return sensor_id
        _SENSOR_MAP_CACHE.pop(key, None)

    row = db.execute(
        text(
            "SELECT TOP 1 s.id "
            "FROM sensors s "
            "JOIN devices d ON d.id = s.device_id "
            "WHERE d.device_uuid = :device_uuid AND s.sensor_uuid = :sensor_uuid"
        ),
        {"device_uuid": str(device_uuid), "sensor_uuid": str(sensor_uuid)},
    ).fetchone()

    if not row:
        return None

    sensor_id = int(row[0])
    expires_at = now + timedelta(seconds=_cache_ttl_seconds())
    _SENSOR_MAP_CACHE[key] = (sensor_id, expires_at)
    return sensor_id


def clear_cache() -> None:
    """Limpia el caché de resolución (útil para testing)."""
    _SENSOR_MAP_CACHE.clear()

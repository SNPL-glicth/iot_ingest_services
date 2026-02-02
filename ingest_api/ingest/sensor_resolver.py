"""Resolución de sensor_id a partir de UUIDs.

Mantiene un caché en memoria para hot paths O(1).

FIX 2026-02-02: Agregado batch resolution y límite LRU para evitar memory leak (P-1).
"""

from __future__ import annotations

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple, List
from uuid import UUID
from collections import OrderedDict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Caché LRU con límite de tamaño
MAX_CACHE_SIZE = 10000
_SENSOR_MAP_CACHE: OrderedDict[Tuple[str, str], Tuple[int, datetime]] = OrderedDict()


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
            # Move to end (LRU)
            _SENSOR_MAP_CACHE.move_to_end(key)
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
    
    # Evitar memory leak: eliminar entradas más antiguas si excede límite
    while len(_SENSOR_MAP_CACHE) >= MAX_CACHE_SIZE:
        _SENSOR_MAP_CACHE.popitem(last=False)
    
    _SENSOR_MAP_CACHE[key] = (sensor_id, expires_at)
    return sensor_id


def resolve_sensor_ids_batch(
    db: Session,
    pairs: List[Tuple[UUID, UUID]],
) -> Dict[Tuple[str, str], Optional[int]]:
    """Resuelve múltiples sensor_ids en batch.
    
    FIX 2026-02-02: Batch resolution para evitar N queries (P-1).
    
    Args:
        db: Sesión de BD
        pairs: Lista de (device_uuid, sensor_uuid)
        
    Returns:
        Dict con (device_uuid, sensor_uuid) -> sensor_id
    """
    now = datetime.now(timezone.utc)
    results: Dict[Tuple[str, str], Optional[int]] = {}
    uncached: List[Tuple[str, str]] = []
    
    # Primero buscar en caché
    for device_uuid, sensor_uuid in pairs:
        key = (str(device_uuid).lower(), str(sensor_uuid).lower())
        cached = _SENSOR_MAP_CACHE.get(key)
        
        if cached is not None:
            sensor_id, expires_at = cached
            if expires_at > now:
                _SENSOR_MAP_CACHE.move_to_end(key)
                results[key] = sensor_id
                continue
            _SENSOR_MAP_CACHE.pop(key, None)
        
        uncached.append(key)
        results[key] = None
    
    if not uncached:
        return results
    
    # Batch query para los no cacheados
    try:
        # Construir query con IN clause
        placeholders = ", ".join(
            f"(:d{i}, :s{i})" for i in range(len(uncached))
        )
        params = {}
        for i, (d, s) in enumerate(uncached):
            params[f"d{i}"] = d
            params[f"s{i}"] = s
        
        rows = db.execute(
            text(
                f"""
                SELECT d.device_uuid, s.sensor_uuid, s.id
                FROM sensors s
                JOIN devices d ON d.id = s.device_id
                WHERE (LOWER(CAST(d.device_uuid AS VARCHAR(36))), LOWER(CAST(s.sensor_uuid AS VARCHAR(36)))) 
                IN ({placeholders})
                """
            ),
            params,
        ).fetchall()
        
        expires_at = now + timedelta(seconds=_cache_ttl_seconds())
        
        for row in rows:
            key = (str(row[0]).lower(), str(row[1]).lower())
            sensor_id = int(row[2])
            results[key] = sensor_id
            
            # Agregar a caché
            while len(_SENSOR_MAP_CACHE) >= MAX_CACHE_SIZE:
                _SENSOR_MAP_CACHE.popitem(last=False)
            _SENSOR_MAP_CACHE[key] = (sensor_id, expires_at)
        
        logger.debug("Batch resolved %d/%d sensors", len(rows), len(uncached))
        
    except Exception as e:
        logger.warning("Batch resolution failed, falling back to individual: %s", e)
        # Fallback a resolución individual
        for key in uncached:
            device_uuid, sensor_uuid = key
            sensor_id = resolve_sensor_id(db, UUID(device_uuid), UUID(sensor_uuid))
            results[key] = sensor_id
    
    return results


def clear_cache() -> None:
    """Limpia el caché de resolución (útil para testing)."""
    _SENSOR_MAP_CACHE.clear()


def get_cache_stats() -> dict:
    """Estadísticas del caché."""
    return {
        "size": len(_SENSOR_MAP_CACHE),
        "max_size": MAX_CACHE_SIZE,
        "ttl_seconds": _cache_ttl_seconds(),
    }

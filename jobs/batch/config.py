"""Batch runner configuration."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RunnerConfig:
    """Configuración del batch runner de producción."""
    window: int
    horizon_minutes: int
    dedupe_minutes: int
    sleep_seconds: float
    once: bool

"""Deadlock retry helper for SQL queries."""

from __future__ import annotations

import logging
import random
import time

from sqlalchemy import text
from sqlalchemy.exc import OperationalError

logger = logging.getLogger(__name__)


def execute_with_retry(conn, query: str, params: dict, max_retries: int = 3):
    """Ejecuta consulta con retry + exponential backoff para deadlocks."""
    for attempt in range(1, max_retries + 1):
        try:
            return conn.execute(text(query), params)
        except OperationalError as e:
            if hasattr(e, "orig") and hasattr(e.orig, "args"):
                if len(e.orig.args) > 0 and isinstance(e.orig.args[0], tuple):
                    error_code = e.orig.args[0][0] if len(e.orig.args[0]) > 0 else None
                else:
                    error_code = None
            else:
                error_code = None
            if error_code == 1205 and attempt < max_retries:
                delay = min(1000 * (2 ** (attempt - 1)), 5000)
                jitter = random.uniform(0, delay * 0.1)
                total_delay = (delay + jitter) / 1000.0
                logger.warning(
                    "Deadlock detectado (intento %d/%d), reintentando en %.2fs...",
                    attempt, max_retries, total_delay,
                )
                time.sleep(total_delay)
                continue
            logger.error("Error ejecutando consulta (intento %d/%d): %s", attempt, max_retries, e)
            raise
    raise OperationalError("Max retries exceeded", {}, None)

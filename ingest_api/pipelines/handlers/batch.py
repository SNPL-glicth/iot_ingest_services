"""Handler para ingesta en lote.

Procesa múltiples lecturas usando BatchInserter para optimizar throughput.

FIX 2026-02-02: Integración con resiliencia (deduplicación, DLQ, retry).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy.orm import Session

from ...core.domain.broker_interface import IReadingBroker
from ..router import ReadingRouter
from ..resilience import MessageDeduplicator, DeadLetterQueue
from ...batch_inserter import get_batch_inserter, BatchInserter


logger = logging.getLogger(__name__)


class BatchReadingHandler:
    """Handler para ingesta de lecturas en lote.
    
    Dos modos de operación:
    1. Modo SP (default): Usa ReadingRouter que delega al SP centralizado
    2. Modo BatchInserter: Usa buffer con flush periódico para alto throughput
    
    FIX 2026-02-02: Ahora soporta deduplicación y DLQ.
    """
    
    def __init__(
        self, 
        db: Session, 
        broker: IReadingBroker,
        use_batch_inserter: bool = False,
        deduplicator: Optional[MessageDeduplicator] = None,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        self._db = db
        self._broker = broker
        self._dedup = deduplicator
        self._dlq = dlq
        self._router = ReadingRouter(db, broker, deduplicator, dlq)
        self._use_batch_inserter = use_batch_inserter
    
    def ingest(
        self,
        rows: List[dict],
    ) -> int:
        """Ingesta múltiples lecturas en lote.
        
        Args:
            rows: Lista de diccionarios con formato:
                  {sensor_id: int, value: float, device_timestamp?: datetime}
        
        Returns:
            Número de lecturas procesadas
        """
        if not rows:
            return 0
        
        if self._use_batch_inserter:
            return self._ingest_with_batch_inserter(rows)
        else:
            return self._ingest_with_router(rows)
    
    def _ingest_with_router(self, rows: List[dict]) -> int:
        """Ingesta usando el router (SP centralizado).
        
        Procesa cada lectura individualmente con clasificación.
        PASO 0: Ahora propaga timestamps precisos (sensor_ts, ingested_ts, sequence)
        """
        count = 0
        for row in rows:
            sensor_id = int(row["sensor_id"])
            value = float(row["value"])
            device_ts = self._parse_device_timestamp(row.get("device_timestamp"))
            
            # PASO 0: Extraer timestamps precisos si están disponibles
            sensor_ts = row.get("sensor_ts")      # Unix epoch preciso del sensor
            ingested_ts = row.get("ingested_ts")  # Unix epoch cuando llegó a ingesta
            sequence = row.get("sequence")        # Número de secuencia
            
            self._router.classify_and_route(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_ts,
                sensor_ts=sensor_ts,
                ingested_ts=ingested_ts,
                sequence=sequence,
            )
            count += 1
        
        return count
    
    def _ingest_with_batch_inserter(self, rows: List[dict]) -> int:
        """Ingesta usando BatchInserter para alto throughput.
        
        Agrega lecturas al buffer del BatchInserter que hace flush periódico.
        """
        batch_inserter = get_batch_inserter()
        if batch_inserter is None:
            logger.warning(
                "BatchInserter not available, falling back to router mode"
            )
            return self._ingest_with_router(rows)
        
        count = 0
        now = datetime.now(timezone.utc)
        
        for row in rows:
            sensor_id = int(row["sensor_id"])
            value = float(row["value"])
            device_ts = self._parse_device_timestamp(row.get("device_timestamp"))
            
            batch_inserter.add(
                sensor_id=sensor_id,
                value=value,
                device_timestamp=device_ts,
                ingest_timestamp=now,
            )
            count += 1
        
        return count
    
    def _parse_device_timestamp(
        self, 
        device_ts: Optional[datetime | str]
    ) -> Optional[datetime]:
        """Parsea el timestamp del dispositivo."""
        if device_ts is None:
            return None
        
        if isinstance(device_ts, datetime):
            return device_ts
        
        if isinstance(device_ts, str):
            try:
                return datetime.fromisoformat(device_ts.replace("Z", "+00:00"))
            except Exception:
                return None
        
        return None

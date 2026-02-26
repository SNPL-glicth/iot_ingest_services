"""Universal ingestion endpoint for non-IoT domains.

This endpoint handles data ingestion for infrastructure, finance, health,
and other non-IoT domains. IoT data continues to use POST /ingest/packets.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status

from ..schemas import DataPacketIn, DataIngestResult
from ..core.domain.data_point import DataPoint
from ..core.domain.classification import DataPointClass
from ..core.classification.universal_classifier import UniversalClassifier
from ..classification.stream_config_repository import StreamConfigRepository
from ..infrastructure.persistence import DomainPersistenceRouter, get_postgres_engine
from ..auth.api_key import require_api_key
from ...common.db import get_engine

logger = logging.getLogger(__name__)

router = APIRouter()


# Dependency injection
def get_domain_router() -> DomainPersistenceRouter:
    """Get domain persistence router instance."""
    sql_server = get_engine()
    postgres = get_postgres_engine()
    return DomainPersistenceRouter(sql_server, postgres)


def get_classifier() -> UniversalClassifier:
    """Get universal classifier instance."""
    postgres = get_postgres_engine()
    config_repo = StreamConfigRepository(postgres)
    return UniversalClassifier(config_repo)


@router.post(
    "/ingest/data",
    response_model=DataIngestResult,
    status_code=status.HTTP_201_CREATED,
    summary="Ingest data from non-IoT domains",
    description=(
        "Universal ingestion endpoint for infrastructure, finance, health, "
        "and other non-IoT domains. For IoT sensor data, use POST /ingest/packets instead."
    ),
)
def ingest_data(
    payload: DataPacketIn,
    request: Request,
    db_router: DomainPersistenceRouter = Depends(get_domain_router),
    classifier: UniversalClassifier = Depends(get_classifier),
    api_key: str = Depends(require_api_key),
) -> DataIngestResult:
    """Ingest data points from non-IoT domains.
    
    This endpoint:
    1. Validates that domain is not 'iot'
    2. Checks if PostgreSQL is configured
    3. Creates DataPoint objects
    4. Classifies each data point
    5. Persists to PostgreSQL
    6. Triggers alerts if needed
    7. Publishes normal data points to ML broker (future)
    
    Args:
        payload: Data packet with source_id, domain, and data points
        request: FastAPI request object
        db_router: Domain persistence router
        classifier: Universal classifier
        api_key: API key for authentication
        
    Returns:
        Ingestion result with counts and details
        
    Raises:
        HTTPException: If domain is not supported or PostgreSQL not configured
    """
    # Verify PostgreSQL is configured for non-IoT domains
    if not db_router.is_domain_supported(payload.domain):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"Domain '{payload.domain}' not supported. "
                "PostgreSQL is not configured. Set POSTGRES_URL environment variable."
            ),
        )
    
    logger.info(
        "[UniversalIngest] Received packet: domain=%s source=%s points=%d",
        payload.domain,
        payload.source_id,
        len(payload.data_points),
    )
    
    results: list[Dict[str, Any]] = []
    inserted = 0
    rejected = 0
    
    for dp_in in payload.data_points:
        try:
            # Create DataPoint
            dp = DataPoint(
                source_id=payload.source_id,
                stream_id=dp_in.stream_id,
                value=dp_in.value,
                timestamp=dp_in.timestamp or datetime.utcnow(),
                domain=payload.domain,
                stream_type=dp_in.stream_type,
                sequence=dp_in.sequence,
                domain_metadata=dp_in.metadata,
            )
            
            # Classify
            classification_result = classifier.classify(dp)
            
            # Persist if not rejected
            if classification_result.should_persist:
                db_router.save_data_point(dp)
                inserted += 1
                
                # Save alert if required
                if classification_result.requires_alert:
                    db_router.save_alert(dp, classification_result)
                
                # TODO: Publish to ML broker if classification is NORMAL
                # This will be implemented when ML service supports universal domains
                
                results.append({
                    "stream_id": dp.stream_id,
                    "status": "ok",
                    "classification": classification_result.classification.value,
                })
            else:
                rejected += 1
                results.append({
                    "stream_id": dp.stream_id,
                    "status": "rejected",
                    "reason": classification_result.reason,
                })
                
        except Exception as e:
            logger.exception(
                "[UniversalIngest] Failed to process data point: stream=%s error=%s",
                dp_in.stream_id,
                e,
            )
            rejected += 1
            results.append({
                "stream_id": dp_in.stream_id,
                "status": "error",
                "reason": str(e),
            })
    
    logger.info(
        "[UniversalIngest] Completed: domain=%s inserted=%d rejected=%d",
        payload.domain,
        inserted,
        rejected,
    )
    
    return DataIngestResult(
        inserted=inserted,
        rejected=rejected,
        results=results,
    )


@router.get(
    "/ingest/health",
    summary="Check universal ingestion health",
    description="Check if PostgreSQL is configured and accessible for multi-domain ingestion",
)
def check_health(
    db_router: DomainPersistenceRouter = Depends(get_domain_router),
) -> Dict[str, Any]:
    """Check health of universal ingestion system.
    
    Returns:
        Health status including PostgreSQL availability
    """
    postgres_available = db_router.is_domain_supported("infrastructure")
    
    return {
        "status": "healthy" if postgres_available else "degraded",
        "iot_domain": "available",
        "multi_domain": "available" if postgres_available else "unavailable",
        "postgres_configured": postgres_available,
        "message": (
            "Multi-domain ingestion ready" if postgres_available
            else "PostgreSQL not configured - IoT-only mode"
        ),
    }

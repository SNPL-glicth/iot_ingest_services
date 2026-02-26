"""CSV ingestion endpoints."""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from datetime import datetime
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Depends, File, Form, HTTPException, UploadFile, status
from pydantic import BaseModel

from ...core.classification.universal_classifier import UniversalClassifier
from ...classification.stream_config_repository import StreamConfigRepository
from ...infrastructure.persistence import DomainPersistenceRouter, get_postgres_engine
from ....common.db import get_engine
from .processor import CSVProcessor

logger = logging.getLogger(__name__)

router = APIRouter()


class CSVJobStatus(BaseModel):
    """CSV import job status."""
    job_id: str
    status: str
    total_rows: int = 0
    processed_rows: int = 0
    inserted_rows: int = 0
    rejected_rows: int = 0
    error: str | None = None
    created_at: datetime
    completed_at: datetime | None = None


def _process_csv_job(
    job_id: str,
    file_path: str,
    source_id: str,
    domain: str,
    timestamp_column: str,
    value_columns: list[str],
    stream_id_prefix: str,
):
    """Background task to process CSV file."""
    postgres = get_postgres_engine()
    if postgres is None:
        logger.error("[CSV] PostgreSQL not configured for job %s", job_id)
        return
    
    try:
        # Update job status to processing
        with postgres.begin() as conn:
            from sqlalchemy import text
            conn.execute(
                text("""
                    UPDATE csv_import_jobs
                    SET status = 'processing', started_at = NOW()
                    WHERE id = :job_id
                """),
                {"job_id": job_id},
            )
        
        # Initialize components
        processor = CSVProcessor()
        config_repo = StreamConfigRepository(postgres)
        classifier = UniversalClassifier(config_repo)
        db_router = DomainPersistenceRouter(get_engine(), postgres)
        
        # Process CSV
        inserted = 0
        rejected = 0
        total = 0
        
        for dp in processor.process(
            file_path,
            source_id,
            domain,
            timestamp_column,
            value_columns,
            stream_id_prefix,
        ):
            total += 1
            
            try:
                result = classifier.classify(dp)
                
                if result.should_persist:
                    db_router.save_data_point(dp)
                    inserted += 1
                    
                    if result.requires_alert:
                        db_router.save_alert(dp, result)
                else:
                    rejected += 1
                
            except Exception as e:
                logger.exception("[CSV] Error processing data point: %s", e)
                rejected += 1
            
            # Update progress every 1000 rows
            if total % 1000 == 0:
                with postgres.begin() as conn:
                    conn.execute(
                        text("""
                            UPDATE csv_import_jobs
                            SET processed_rows = :processed, inserted_rows = :inserted,
                                rejected_rows = :rejected
                            WHERE id = :job_id
                        """),
                        {
                            "job_id": job_id,
                            "processed": total,
                            "inserted": inserted,
                            "rejected": rejected,
                        },
                    )
        
        # Mark as completed
        with postgres.begin() as conn:
            conn.execute(
                text("""
                    UPDATE csv_import_jobs
                    SET status = 'completed', completed_at = NOW(),
                        total_rows = :total, processed_rows = :processed,
                        inserted_rows = :inserted, rejected_rows = :rejected
                    WHERE id = :job_id
                """),
                {
                    "job_id": job_id,
                    "total": total,
                    "processed": total,
                    "inserted": inserted,
                    "rejected": rejected,
                },
            )
        
        logger.info(
            "[CSV] Job completed: job_id=%s total=%d inserted=%d rejected=%d",
            job_id, total, inserted, rejected,
        )
        
    except Exception as e:
        logger.exception("[CSV] Job failed: job_id=%s error=%s", job_id, e)
        
        # Mark as failed
        with postgres.begin() as conn:
            conn.execute(
                text("""
                    UPDATE csv_import_jobs
                    SET status = 'failed', completed_at = NOW(), error = :error
                    WHERE id = :job_id
                """),
                {"job_id": job_id, "error": str(e)},
            )
    
    finally:
        # Clean up temp file
        try:
            os.remove(file_path)
        except Exception:
            pass


@router.post(
    "/ingest/csv",
    response_model=Dict[str, Any],
    status_code=status.HTTP_202_ACCEPTED,
    summary="Import data from CSV file",
)
async def ingest_csv(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    source_id: str = Form(...),
    domain: str = Form(...),
    timestamp_column: str = Form(...),
    value_columns: str = Form(...),
    stream_id_prefix: str = Form(""),
):
    """Import data from CSV file.
    
    Feature flag: FF_CSV_ENABLED (default: false)
    Only accepts domain != 'iot'
    
    Args:
        file: CSV file to import
        source_id: Source identifier
        domain: Domain identifier (not 'iot')
        timestamp_column: Name of timestamp column
        value_columns: JSON array of value column names
        stream_id_prefix: Optional prefix for stream IDs
        
    Returns:
        Job ID and status
    """
    # Check feature flag
    if not os.getenv("FF_CSV_ENABLED", "false").lower() in ("true", "1", "yes", "on"):
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="CSV import disabled (FF_CSV_ENABLED=false)",
        )
    
    # Block IoT domain
    if domain.lower() == "iot":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="domain='iot' not allowed in CSV import",
        )
    
    # Check PostgreSQL
    postgres = get_postgres_engine()
    if postgres is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PostgreSQL not configured",
        )
    
    # Parse value columns
    import json
    try:
        value_cols = json.loads(value_columns)
        if not isinstance(value_cols, list) or not value_cols:
            raise ValueError("value_columns must be non-empty array")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid value_columns: {e}",
        )
    
    # Save uploaded file to temp location
    job_id = str(uuid.uuid4())
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    
    try:
        content = await file.read()
        temp_file.write(content)
        temp_file.close()
        
        # Create job record
        with postgres.begin() as conn:
            from sqlalchemy import text
            conn.execute(
                text("""
                    INSERT INTO csv_import_jobs (
                        id, source_id, domain, filename, status, created_at
                    ) VALUES (
                        :job_id, :source_id, :domain, :filename, 'pending', NOW()
                    )
                """),
                {
                    "job_id": job_id,
                    "source_id": source_id,
                    "domain": domain,
                    "filename": file.filename,
                },
            )
        
        # Schedule background processing
        background_tasks.add_task(
            _process_csv_job,
            job_id,
            temp_file.name,
            source_id,
            domain,
            timestamp_column,
            value_cols,
            stream_id_prefix,
        )
        
        logger.info(
            "[CSV] Job created: job_id=%s domain=%s source=%s file=%s",
            job_id, domain, source_id, file.filename,
        )
        
        return {
            "job_id": job_id,
            "status": "processing",
            "message": "CSV import started in background",
        }
        
    except Exception as e:
        # Clean up on error
        try:
            os.remove(temp_file.name)
        except Exception:
            pass
        
        logger.exception("[CSV] Error creating job: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


@router.get(
    "/ingest/csv/jobs/{job_id}",
    response_model=CSVJobStatus,
    summary="Get CSV import job status",
)
async def get_csv_job_status(job_id: str):
    """Get status of CSV import job.
    
    Args:
        job_id: Job identifier
        
    Returns:
        Job status
    """
    postgres = get_postgres_engine()
    if postgres is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="PostgreSQL not configured",
        )
    
    try:
        with postgres.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(
                text("""
                    SELECT id, status, total_rows, processed_rows, inserted_rows,
                           rejected_rows, error, created_at, completed_at
                    FROM csv_import_jobs
                    WHERE id = :job_id
                """),
                {"job_id": job_id},
            ).fetchone()
            
            if not result:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Job {job_id} not found",
                )
            
            return CSVJobStatus(
                job_id=result[0],
                status=result[1],
                total_rows=result[2] or 0,
                processed_rows=result[3] or 0,
                inserted_rows=result[4] or 0,
                rejected_rows=result[5] or 0,
                error=result[6],
                created_at=result[7],
                completed_at=result[8],
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("[CSV] Error fetching job status: %s", e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )

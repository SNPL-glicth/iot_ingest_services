from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class SensorReadingIn(BaseModel):
    # Legacy endpoint (by internal ID)
    sensor_id: int = Field(..., ge=1)
    value: float
    timestamp: Optional[datetime] = None


class BulkSensorReadingsIn(BaseModel):
    readings: List[SensorReadingIn] = Field(default_factory=list)


class SensorReadingUuidIn(BaseModel):
    sensor_uuid: UUID
    value: float
    sensor_ts: Optional[float] = None  # Unix epoch from sensor (precise timing)
    sequence: Optional[int] = None     # Sequence number for ordering


class DevicePacketIn(BaseModel):
    device_uuid: UUID
    ts: Optional[datetime] = None  # Legacy: device timestamp as ISO string
    readings: List[SensorReadingUuidIn] = Field(default_factory=list)


class IngestResult(BaseModel):
    inserted: int


class PacketIngestResult(IngestResult):
    unknown_sensors: List[UUID] = Field(default_factory=list)
    ingested_ts: Optional[float] = None  # When ingestion received the packet


class SensorFinalState(str, Enum):
    ALERT = "alert"
    WARNING = "warning"
    PREDICTION = "prediction"
    UNKNOWN = "unknown"


class ActiveAlert(BaseModel):
    id: int
    sensor_id: int
    device_id: int
    threshold_id: int
    severity: str
    status: str
    triggered_value: float
    triggered_at: datetime


class ActiveWarning(BaseModel):
    id: int
    sensor_id: int
    device_id: int
    event_type: str
    event_code: str
    status: str
    created_at: datetime
    title: Optional[str] = None
    message: Optional[str] = None
    payload: Optional[dict] = None


class CurrentPrediction(BaseModel):
    id: int
    sensor_id: int
    model_id: int
    predicted_value: float
    confidence: float
    predicted_at: datetime
    target_timestamp: datetime


class SensorConsolidatedStatus(BaseModel):
    sensor_id: int
    final_state: SensorFinalState
    alert_active: Optional[ActiveAlert] = None
    warning_active: Optional[ActiveWarning] = None
    prediction_current: Optional[CurrentPrediction] = None


# ============================================================================
# Universal Ingestion Schemas (Multi-Domain)
# These schemas support non-IoT domains (infrastructure, finance, health, etc.)
# IoT domain continues to use DevicePacketIn above
# ============================================================================


class DataPointIn(BaseModel):
    """Single data point for universal ingestion."""
    stream_id: str
    value: float
    timestamp: Optional[datetime] = None
    stream_type: Optional[str] = None
    sequence: Optional[int] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class DataPacketIn(BaseModel):
    """Batch of data points from a single source.
    
    Note: domain cannot be 'iot' - use POST /ingest/packets for IoT data.
    """
    source_id: str
    domain: str = Field(..., pattern=r"^(?!iot$).*")
    data_points: List[DataPointIn]
    
    @field_validator("domain")
    @classmethod
    def domain_not_iot(cls, v: str) -> str:
        """Ensure domain is not 'iot'."""
        if v.lower() == "iot":
            raise ValueError(
                "domain='iot' is not allowed in universal ingestion. "
                "Use POST /ingest/packets for IoT data."
            )
        return v


class DataIngestResult(BaseModel):
    """Result of universal data ingestion."""
    inserted: int
    rejected: int = 0
    results: List[Dict[str, Any]] = Field(default_factory=list)

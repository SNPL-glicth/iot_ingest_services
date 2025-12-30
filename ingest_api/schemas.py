from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


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


class DevicePacketIn(BaseModel):
    device_uuid: UUID
    ts: Optional[datetime] = None
    readings: List[SensorReadingUuidIn] = Field(default_factory=list)


class IngestResult(BaseModel):
    inserted: int


class PacketIngestResult(IngestResult):
    unknown_sensors: List[UUID] = Field(default_factory=list)


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

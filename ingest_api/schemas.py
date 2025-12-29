from __future__ import annotations

from datetime import datetime
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

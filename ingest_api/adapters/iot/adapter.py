"""IoTAdapter - Bridge bidireccional entre sistema IoT legacy y arquitectura universal.

Este adapter permite la interoperabilidad entre:
- Reading (modelo IoT legacy con sensor_id:int)
- DataPoint (modelo universal con series_id:str)

IMPORTANTE: Este adapter IMPORTA desde los archivos IoT originales SIN MODIFICARLOS.
Los archivos IoT permanecen en su ubicación original intactos.
"""

from __future__ import annotations

from datetime import datetime
from typing import List, Optional
from uuid import UUID

# Imports desde archivos IoT ORIGINALES (no modificados, no movidos)
from ...core.domain.reading import Reading
from ...schemas import DevicePacketIn

# Imports desde contratos universales NUEVOS
from ...core.domain.data_point import DataPoint
from ...core.domain.series_id import SeriesIdMapper


class IoTAdapter:
    """Adapter bidireccional: Reading ↔ DataPoint.
    
    Permite que el código IoT legacy siga funcionando sin cambios
    mientras se integra con la nueva arquitectura universal.
    """
    
    @staticmethod
    def reading_to_datapoint(reading: Reading) -> DataPoint:
        """Convierte Reading (IoT legacy) a DataPoint (universal).
        
        Args:
            reading: Lectura IoT con sensor_id:int
            
        Returns:
            DataPoint con series_id:str y legacy_sensor_id preservado
            
        Example:
            >>> reading = Reading(sensor_id=42, value=23.5, timestamp=now)
            >>> dp = IoTAdapter.reading_to_datapoint(reading)
            >>> dp.series_id
            'iot:sensor:42'
            >>> dp.legacy_sensor_id
            42
        """
        series_id = SeriesIdMapper.iot_sensor_to_series_id(reading.sensor_id)
        
        return DataPoint(
            series_id=series_id,
            value=reading.value,
            timestamp=reading.timestamp,
            domain="iot",
            source_id=f"sensor_{reading.sensor_id}",
            stream_id=str(reading.sensor_id),
            stream_type=reading.sensor_type,
            legacy_sensor_id=reading.sensor_id,
            metadata={
                "device_uuid": reading.device_uuid,
                "sensor_uuid": reading.sensor_uuid,
                "sequence": reading.sequence,
                "device_timestamp": reading.device_timestamp.isoformat() if reading.device_timestamp else None,
            },
            sequence=reading.sequence,
        )
    
    @staticmethod
    def datapoint_to_reading(dp: DataPoint) -> Reading:
        """Convierte DataPoint (universal) a Reading (IoT legacy).
        
        Args:
            dp: DataPoint con legacy_sensor_id
            
        Returns:
            Reading compatible con código IoT legacy
            
        Raises:
            ValueError: Si el DataPoint no tiene legacy_sensor_id
            
        Example:
            >>> dp = DataPoint(series_id="iot:sensor:42", value=23.5, 
            ...                timestamp=now, legacy_sensor_id=42)
            >>> reading = IoTAdapter.datapoint_to_reading(dp)
            >>> reading.sensor_id
            42
        """
        if dp.legacy_sensor_id is None:
            raise ValueError(
                f"DataPoint must have legacy_sensor_id for IoT conversion. "
                f"series_id={dp.series_id}"
            )
        
        # Extraer device_timestamp de metadata si existe
        device_timestamp = None
        if "device_timestamp" in dp.metadata and dp.metadata["device_timestamp"]:
            try:
                device_timestamp = datetime.fromisoformat(dp.metadata["device_timestamp"])
            except (ValueError, TypeError):
                pass
        
        return Reading(
            sensor_id=dp.legacy_sensor_id,
            value=dp.value,
            timestamp=dp.timestamp,
            device_timestamp=device_timestamp,
            sensor_type=dp.stream_type or "unknown",
            device_uuid=dp.metadata.get("device_uuid"),
            sensor_uuid=dp.metadata.get("sensor_uuid"),
            sequence=dp.sequence,
        )
    
    @staticmethod
    def from_device_packet(packet: DevicePacketIn) -> List[DataPoint]:
        """Convierte DevicePacketIn (esquema IoT) a lista de DataPoints.
        
        Args:
            packet: Paquete de lecturas desde dispositivo IoT
            
        Returns:
            Lista de DataPoints, uno por cada lectura en el paquete
            
        Note:
            Esta conversión NO resuelve sensor_uuid → sensor_id.
            Esa resolución debe hacerse antes de llamar a este método.
            Este método solo crea DataPoints con la información disponible.
        """
        data_points = []
        
        for reading_in in packet.readings:
            # Crear DataPoint con información disponible
            # sensor_id se resolverá después en el pipeline
            dp = DataPoint(
                series_id=f"iot:device:{packet.device_uuid}:sensor:{reading_in.sensor_uuid}",
                value=float(reading_in.value),
                timestamp=packet.ts or datetime.utcnow(),
                domain="iot",
                source_id=str(packet.device_uuid),
                stream_id=str(reading_in.sensor_uuid),
                stream_type="unknown",  # Se resuelve después
                metadata={
                    "device_uuid": str(packet.device_uuid),
                    "sensor_uuid": str(reading_in.sensor_uuid),
                    "sensor_ts": reading_in.sensor_ts,
                    "sequence": reading_in.sequence,
                },
                sequence=reading_in.sequence,
            )
            data_points.append(dp)
        
        return data_points
    
    @staticmethod
    def enrich_with_sensor_id(dp: DataPoint, sensor_id: int, sensor_type: str = "unknown") -> DataPoint:
        """Enriquece un DataPoint con sensor_id resuelto.
        
        Args:
            dp: DataPoint original (puede tener solo UUIDs)
            sensor_id: ID numérico del sensor (resuelto desde UUID)
            sensor_type: Tipo de sensor
            
        Returns:
            DataPoint enriquecido con legacy_sensor_id y series_id actualizado
        """
        dp.legacy_sensor_id = sensor_id
        dp.series_id = SeriesIdMapper.iot_sensor_to_series_id(sensor_id)
        dp.stream_id = str(sensor_id)
        dp.stream_type = sensor_type
        return dp

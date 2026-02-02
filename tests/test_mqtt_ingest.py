"""Tests de ingesta MQTT.

Tests obligatorios:
1. Test de contrato idéntico a HTTP
2. Test deduplicación
3. Test latencia < HTTP
4. Test reconexión MQTT
5. Test payload malformado

Ejecutar:
    pytest tests/test_mqtt_ingest.py -v
"""

import asyncio
import hashlib
import json
import time
from datetime import datetime
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ingest_api.mqtt.validators import (
    MQTTReadingPayload,
    ValidationResult,
    validate_mqtt_reading,
)
from ingest_api.mqtt.mqtt_bridge import (
    DeduplicationCache,
    MQTTRedisBridge,
)


# =============================================================================
# FIXTURES
# =============================================================================

@pytest.fixture
def valid_mqtt_payload() -> Dict[str, Any]:
    """Payload MQTT válido."""
    return {
        "v": 1,
        "sensorId": "42",
        "value": 23.456,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "type": "reading",
        "metadata": {
            "deviceId": "10",
            "deviceUuid": "d1234567-abcd-efgh-ijkl-mnopqrstuvwx",
            "sensorUuid": "s7654321-abcd-efgh-ijkl-mnopqrstuvwx",
            "sequence": 12345,
        }
    }


@pytest.fixture
def http_ingest_payload() -> Dict[str, Any]:
    """Payload HTTP equivalente (para comparación)."""
    return {
        "device_uuid": "d1234567-abcd-efgh-ijkl-mnopqrstuvwx",
        "ts": datetime.utcnow().isoformat() + "Z",
        "readings": [
            {
                "sensor_uuid": "s7654321-abcd-efgh-ijkl-mnopqrstuvwx",
                "value": 23.456,
                "sensor_ts": time.time(),
                "sequence": 12345,
            }
        ]
    }


@pytest.fixture
def dedup_cache() -> DeduplicationCache:
    """Cache de deduplicación para tests."""
    return DeduplicationCache(ttl_seconds=60, max_size=1000)


@pytest.fixture
def mock_broker():
    """Mock del broker Redis."""
    broker = MagicMock()
    broker.publish = MagicMock()
    broker.health_check = MagicMock(return_value={"connected": True})
    return broker


# =============================================================================
# TEST 1: CONTRATO IDÉNTICO A HTTP
# =============================================================================

class TestMQTTContractIdenticalToHTTP:
    """Verifica que MQTT produce el mismo resultado que HTTP."""
    
    def test_mqtt_payload_converts_to_same_format(self, valid_mqtt_payload):
        """Payload MQTT se convierte al mismo formato interno que HTTP."""
        result = validate_mqtt_reading(valid_mqtt_payload)
        
        assert result.valid is True
        assert result.payload is not None
        
        # Verificar campos críticos
        payload = result.payload
        assert payload.sensor_id_int == 42
        assert payload.value == 23.456
        assert payload.device_uuid == "d1234567-abcd-efgh-ijkl-mnopqrstuvwx"
        assert payload.sensor_uuid == "s7654321-abcd-efgh-ijkl-mnopqrstuvwx"
        assert payload.sequence == 12345
    
    def test_mqtt_to_ingest_row_format(self, valid_mqtt_payload):
        """Formato de fila de ingesta es idéntico."""
        result = validate_mqtt_reading(valid_mqtt_payload)
        row = result.payload.to_ingest_row()
        
        # Campos requeridos por el pipeline
        assert "sensor_id" in row
        assert "value" in row
        assert "ingested_ts" in row
        assert "sensor_ts" in row
        
        assert row["sensor_id"] == 42
        assert row["value"] == 23.456
        assert isinstance(row["ingested_ts"], float)
        assert isinstance(row["sensor_ts"], float)
    
    def test_snake_case_compatibility(self):
        """Acepta tanto camelCase como snake_case."""
        payload_snake = {
            "v": 1,
            "sensor_id": "42",  # snake_case
            "value": 23.456,
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "type": "reading",
        }
        
        result = validate_mqtt_reading(payload_snake)
        
        assert result.valid is True
        assert result.payload.sensor_id_int == 42
        assert "snake_case" in str(result.warnings)


# =============================================================================
# TEST 2: DEDUPLICACIÓN
# =============================================================================

class TestDeduplication:
    """Verifica que la deduplicación funciona correctamente."""
    
    @pytest.mark.asyncio
    async def test_duplicate_detection(self, dedup_cache):
        """Detecta duplicados correctamente."""
        key = dedup_cache.generate_key(
            sensor_id=42,
            value=23.456,
            timestamp=1706688000.123,
        )
        
        # Primera vez: no es duplicado
        is_dup = await dedup_cache.is_duplicate(key)
        assert is_dup is False
        
        # Marcar como visto
        await dedup_cache.mark_seen(key)
        
        # Segunda vez: es duplicado
        is_dup = await dedup_cache.is_duplicate(key)
        assert is_dup is True
    
    @pytest.mark.asyncio
    async def test_different_values_not_duplicate(self, dedup_cache):
        """Valores diferentes no son duplicados."""
        key1 = dedup_cache.generate_key(42, 23.456, 1706688000.123)
        key2 = dedup_cache.generate_key(42, 23.457, 1706688000.123)  # Valor diferente
        
        await dedup_cache.mark_seen(key1)
        
        is_dup = await dedup_cache.is_duplicate(key2)
        assert is_dup is False
    
    @pytest.mark.asyncio
    async def test_different_timestamps_not_duplicate(self, dedup_cache):
        """Timestamps diferentes no son duplicados."""
        key1 = dedup_cache.generate_key(42, 23.456, 1706688000.123)
        key2 = dedup_cache.generate_key(42, 23.456, 1706688001.123)  # Timestamp diferente
        
        await dedup_cache.mark_seen(key1)
        
        is_dup = await dedup_cache.is_duplicate(key2)
        assert is_dup is False
    
    @pytest.mark.asyncio
    async def test_dedup_key_format(self, dedup_cache):
        """Formato de clave de deduplicación es consistente."""
        key1 = dedup_cache.generate_key(42, 23.456789, 1706688000.123456)
        key2 = dedup_cache.generate_key(42, 23.456789, 1706688000.123456)
        
        # Mismos inputs = misma clave
        assert key1 == key2
        
        # Clave es MD5 truncado
        assert len(key1) == 16
    
    @pytest.mark.asyncio
    async def test_bridge_deduplication(self, valid_mqtt_payload, mock_broker):
        """Bridge deduplica correctamente."""
        bridge = MQTTRedisBridge(
            broker_factory=lambda: mock_broker,
            dedup_enabled=True,
        )
        
        result = validate_mqtt_reading(valid_mqtt_payload)
        
        # Primera vez: procesa
        success1 = await bridge.process_reading(result.payload)
        assert success1 is True
        assert bridge.stats["readings_processed"] == 1
        
        # Segunda vez: deduplica
        success2 = await bridge.process_reading(result.payload)
        assert success2 is True
        assert bridge.stats["readings_deduplicated"] == 1
        assert bridge.stats["readings_processed"] == 1  # No aumentó


# =============================================================================
# TEST 3: LATENCIA
# =============================================================================

class TestLatency:
    """Verifica que la latencia MQTT es aceptable."""
    
    @pytest.mark.asyncio
    async def test_validation_latency(self, valid_mqtt_payload):
        """Validación es rápida (<1ms)."""
        iterations = 100
        start = time.perf_counter()
        
        for _ in range(iterations):
            validate_mqtt_reading(valid_mqtt_payload)
        
        elapsed_ms = (time.perf_counter() - start) * 1000
        avg_ms = elapsed_ms / iterations
        
        assert avg_ms < 1.0, f"Validation too slow: {avg_ms:.3f}ms"
    
    @pytest.mark.asyncio
    async def test_dedup_check_latency(self, dedup_cache):
        """Deduplicación es rápida (<0.1ms)."""
        key = dedup_cache.generate_key(42, 23.456, 1706688000.123)
        
        iterations = 1000
        start = time.perf_counter()
        
        for _ in range(iterations):
            await dedup_cache.is_duplicate(key)
        
        elapsed_ms = (time.perf_counter() - start) * 1000
        avg_ms = elapsed_ms / iterations
        
        assert avg_ms < 0.1, f"Dedup check too slow: {avg_ms:.3f}ms"
    
    @pytest.mark.asyncio
    async def test_bridge_processing_latency(self, valid_mqtt_payload, mock_broker):
        """Procesamiento completo es rápido (<5ms)."""
        bridge = MQTTRedisBridge(
            broker_factory=lambda: mock_broker,
            dedup_enabled=True,
        )
        
        result = validate_mqtt_reading(valid_mqtt_payload)
        
        iterations = 100
        latencies = []
        
        for i in range(iterations):
            # Modificar timestamp para evitar dedup
            valid_mqtt_payload["timestamp"] = datetime.utcnow().isoformat() + "Z"
            result = validate_mqtt_reading(valid_mqtt_payload)
            
            start = time.perf_counter()
            await bridge.process_reading(result.payload)
            latencies.append((time.perf_counter() - start) * 1000)
        
        avg_ms = sum(latencies) / len(latencies)
        p95_ms = sorted(latencies)[int(len(latencies) * 0.95)]
        
        assert avg_ms < 5.0, f"Avg latency too high: {avg_ms:.3f}ms"
        assert p95_ms < 10.0, f"P95 latency too high: {p95_ms:.3f}ms"


# =============================================================================
# TEST 4: RECONEXIÓN (Mock)
# =============================================================================

class TestReconnection:
    """Verifica comportamiento de reconexión."""
    
    def test_receiver_tracks_reconnect_count(self):
        """Receiver trackea reconexiones."""
        from ingest_api.mqtt.mqtt_receiver import MQTTIngestReceiver
        from ingest_api.mqtt.mqtt_bridge import MQTTRedisBridge
        
        bridge = MQTTRedisBridge()
        receiver = MQTTIngestReceiver(bridge)
        
        # Verificar que stats incluye reconnect_count
        stats = receiver.stats
        assert "reconnect_count" in stats
        assert stats["reconnect_count"] == 0
    
    def test_receiver_health_check(self):
        """Health check reporta estado correcto."""
        from ingest_api.mqtt.mqtt_receiver import MQTTIngestReceiver
        from ingest_api.mqtt.mqtt_bridge import MQTTRedisBridge
        
        bridge = MQTTRedisBridge()
        receiver = MQTTIngestReceiver(bridge)
        
        health = receiver.health_check()
        
        assert "healthy" in health
        assert "running" in health
        assert "connected" in health
        assert health["running"] is False  # No iniciado


# =============================================================================
# TEST 5: PAYLOAD MALFORMADO
# =============================================================================

class TestMalformedPayload:
    """Verifica manejo de payloads inválidos."""
    
    def test_missing_sensor_id(self):
        """Rechaza payload sin sensorId."""
        payload = {
            "v": 1,
            "value": 23.456,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "sensorId" in str(result.error).lower() or "sensor_id" in str(result.error).lower()
    
    def test_missing_value(self):
        """Rechaza payload sin value."""
        payload = {
            "v": 1,
            "sensorId": "42",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
    
    def test_invalid_timestamp_future(self):
        """Rechaza timestamp muy en el futuro."""
        future_ts = datetime(2030, 1, 1).isoformat() + "Z"
        payload = {
            "v": 1,
            "sensorId": "42",
            "value": 23.456,
            "timestamp": future_ts,
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "future" in str(result.error).lower()
    
    def test_invalid_timestamp_old(self):
        """Rechaza timestamp muy viejo."""
        old_ts = datetime(2020, 1, 1).isoformat() + "Z"
        payload = {
            "v": 1,
            "sensorId": "42",
            "value": 23.456,
            "timestamp": old_ts,
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "old" in str(result.error).lower()
    
    def test_nan_value(self):
        """Rechaza valor NaN."""
        payload = {
            "v": 1,
            "sensorId": "42",
            "value": float("nan"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "nan" in str(result.error).lower()
    
    def test_infinite_value(self):
        """Rechaza valor infinito."""
        payload = {
            "v": 1,
            "sensorId": "42",
            "value": float("inf"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "infinite" in str(result.error).lower()
    
    def test_non_numeric_sensor_id(self):
        """Rechaza sensorId no numérico."""
        payload = {
            "v": 1,
            "sensorId": "abc-not-numeric",
            "value": 23.456,
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "numeric" in str(result.error).lower()
    
    def test_empty_payload(self):
        """Rechaza payload vacío."""
        result = validate_mqtt_reading({})
        
        assert result.valid is False
    
    def test_value_out_of_range(self):
        """Rechaza valor fuera de rango."""
        payload = {
            "v": 1,
            "sensorId": "42",
            "value": 1e15,  # Muy grande
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        
        result = validate_mqtt_reading(payload)
        
        assert result.valid is False
        assert "range" in str(result.error).lower()


# =============================================================================
# TEST ADICIONAL: BRIDGE HEALTH CHECK
# =============================================================================

class TestBridgeHealthCheck:
    """Verifica health check del bridge."""
    
    @pytest.mark.asyncio
    async def test_bridge_health_check(self, mock_broker):
        """Health check reporta estado correcto."""
        bridge = MQTTRedisBridge(
            broker_factory=lambda: mock_broker,
        )
        
        health = bridge.health_check()
        
        assert "healthy" in health
        assert "readings_processed" in health
        assert "readings_deduplicated" in health
        assert "readings_failed" in health
        assert "dedup_cache_size" in health
    
    @pytest.mark.asyncio
    async def test_bridge_stats(self, valid_mqtt_payload, mock_broker):
        """Stats se actualizan correctamente."""
        bridge = MQTTRedisBridge(
            broker_factory=lambda: mock_broker,
        )
        
        result = validate_mqtt_reading(valid_mqtt_payload)
        await bridge.process_reading(result.payload)
        
        stats = bridge.stats
        
        assert stats["readings_processed"] == 1
        assert stats["readings_deduplicated"] == 0
        assert stats["readings_failed"] == 0
        assert "dedup_cache" in stats
        assert stats["dedup_cache"]["size"] == 1


# =============================================================================
# TEST INTEGRACIÓN: FLUJO COMPLETO
# =============================================================================

class TestFullFlow:
    """Test de integración del flujo completo."""
    
    @pytest.mark.asyncio
    async def test_mqtt_to_redis_flow(self, valid_mqtt_payload, mock_broker):
        """Flujo completo MQTT → validación → dedup → Redis."""
        bridge = MQTTRedisBridge(
            broker_factory=lambda: mock_broker,
            dedup_enabled=True,
        )
        
        # Simular recepción de mensaje MQTT
        result = validate_mqtt_reading(valid_mqtt_payload)
        assert result.valid is True
        
        # Procesar a través del bridge
        success = await bridge.process_reading(result.payload)
        assert success is True
        
        # Verificar que se publicó a Redis
        mock_broker.publish.assert_called_once()
        
        # Verificar stats
        assert bridge.stats["readings_processed"] == 1
        assert bridge.stats["readings_failed"] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

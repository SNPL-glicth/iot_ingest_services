"""WebSocket handler for real-time multi-domain data ingestion.

This transport enables bidirectional streaming of data points with
backpressure control and session management.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any, Dict, List

from fastapi import WebSocket, WebSocketDisconnect, status

from ...core.domain.data_point import DataPoint
from ...core.domain.classification import DataPointClass
from ...core.classification.universal_classifier import UniversalClassifier
from ...classification.stream_config_repository import StreamConfigRepository
from ...infrastructure.persistence import DomainPersistenceRouter, get_postgres_engine
from ....common.db import get_engine

logger = logging.getLogger(__name__)


async def websocket_ingest(websocket: WebSocket):
    """WebSocket endpoint for streaming data ingestion.
    
    Protocol:
    1. Client → {type: "connect", source_id, domain, api_key}
    2. Server → {type: "connected", session_id}
    3. Client → {type: "data", batch: [{stream_id, value, timestamp, sequence}]}
    4. Server → {type: "ack", sequence_up_to, rejected: []}
    5. Client → {type: "disconnect"}
    
    Feature flag: FF_WEBSOCKET_ENABLED (default: false)
    Only active for non-IoT domains.
    """
    # Check feature flag
    if not os.getenv("FF_WEBSOCKET_ENABLED", "false").lower() in ("true", "1", "yes", "on"):
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="WebSocket transport disabled")
        return
    
    await websocket.accept()
    
    session_id = None
    source_id = None
    domain = None
    max_pending = 100
    pending_count = 0
    
    try:
        # 1. Handshake
        handshake = await websocket.receive_json()
        
        if handshake.get("type") != "connect":
            await websocket.send_json({
                "type": "error",
                "error": "Expected 'connect' message",
            })
            await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
            return
        
        source_id = handshake.get("source_id")
        domain = handshake.get("domain")
        api_key = handshake.get("api_key")
        
        if not source_id or not domain or not api_key:
            await websocket.send_json({
                "type": "error",
                "error": "Missing source_id, domain, or api_key",
            })
            await websocket.close(code=status.WS_1003_UNSUPPORTED_DATA)
            return
        
        # Block IoT domain
        if domain.lower() == "iot":
            await websocket.send_json({
                "type": "error",
                "error": "domain='iot' not allowed in WebSocket transport",
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Validate API key (simple check - enhance as needed)
        # TODO: Integrate with proper auth system
        expected_key = os.getenv("API_KEY", "")
        if api_key != expected_key:
            await websocket.send_json({
                "type": "error",
                "error": "Invalid API key",
            })
            await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
            return
        
        # Check PostgreSQL availability
        postgres = get_postgres_engine()
        if postgres is None:
            await websocket.send_json({
                "type": "error",
                "error": "PostgreSQL not configured",
            })
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
            return
        
        # Create session
        import uuid
        session_id = str(uuid.uuid4())
        
        await websocket.send_json({
            "type": "connected",
            "session_id": session_id,
        })
        
        logger.info(
            "[WebSocket] Session connected: session=%s domain=%s source=%s",
            session_id, domain, source_id,
        )
        
        # Initialize components
        config_repo = StreamConfigRepository(postgres)
        classifier = UniversalClassifier(config_repo)
        db_router = DomainPersistenceRouter(get_engine(), postgres)
        
        # 3. Data loop
        while True:
            message = await websocket.receive_json()
            
            msg_type = message.get("type")
            
            if msg_type == "disconnect":
                logger.info("[WebSocket] Client requested disconnect: session=%s", session_id)
                break
            
            if msg_type != "data":
                await websocket.send_json({
                    "type": "error",
                    "error": f"Unknown message type: {msg_type}",
                })
                continue
            
            # Backpressure check
            if pending_count >= max_pending:
                await websocket.send_json({
                    "type": "backpressure",
                    "message": "Too many pending messages, slow down",
                })
                continue
            
            batch = message.get("batch", [])
            if not batch:
                await websocket.send_json({
                    "type": "error",
                    "error": "Empty batch",
                })
                continue
            
            # Process batch
            rejected: List[Dict[str, Any]] = []
            max_sequence = 0
            
            for item in batch:
                pending_count += 1
                try:
                    stream_id = item.get("stream_id")
                    value = item.get("value")
                    
                    if not stream_id or value is None:
                        rejected.append({
                            "stream_id": stream_id or "unknown",
                            "reason": "Missing stream_id or value",
                        })
                        continue
                    
                    # Parse timestamp
                    timestamp_str = item.get("timestamp")
                    if timestamp_str:
                        try:
                            timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                        except Exception:
                            timestamp = datetime.utcnow()
                    else:
                        timestamp = datetime.utcnow()
                    
                    sequence = item.get("sequence")
                    if sequence and sequence > max_sequence:
                        max_sequence = sequence
                    
                    # Create DataPoint
                    dp = DataPoint(
                        source_id=source_id,
                        stream_id=stream_id,
                        value=float(value),
                        timestamp=timestamp,
                        domain=domain,
                        sequence=sequence,
                        domain_metadata=item.get("metadata", {}),
                    )
                    
                    # Classify
                    result = classifier.classify(dp)
                    
                    # Persist
                    if result.should_persist:
                        db_router.save_data_point(dp)
                        
                        if result.requires_alert:
                            db_router.save_alert(dp, result)
                    else:
                        rejected.append({
                            "stream_id": stream_id,
                            "reason": result.reason,
                        })
                    
                except Exception as e:
                    logger.exception("[WebSocket] Error processing item: %s", e)
                    rejected.append({
                        "stream_id": item.get("stream_id", "unknown"),
                        "reason": str(e),
                    })
                finally:
                    pending_count -= 1
            
            # Send ACK
            await websocket.send_json({
                "type": "ack",
                "sequence_up_to": max_sequence if max_sequence > 0 else None,
                "rejected": rejected,
                "processed": len(batch) - len(rejected),
            })
        
        # Clean disconnect
        await websocket.close(code=status.WS_1000_NORMAL_CLOSURE)
        logger.info("[WebSocket] Session closed normally: session=%s", session_id)
        
    except WebSocketDisconnect:
        logger.info("[WebSocket] Client disconnected: session=%s", session_id)
    except Exception as e:
        logger.exception("[WebSocket] Session error: session=%s error=%s", session_id, e)
        try:
            await websocket.close(code=status.WS_1011_INTERNAL_ERROR)
        except Exception:
            pass

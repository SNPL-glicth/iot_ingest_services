"""Universal MQTT Receiver - Receptor MQTT para dominios universales.

IMPORTANTE: Este receiver NO interfiere con el MQTT receiver IoT existente.
- Usa un cliente MQTT separado (nueva conexión)
- Suscribe a topics diferentes (+/+/+/data vs iot/sensors/+/readings)
- Solo se inicia si FF_MQTT_UNIVERSAL=true
"""

from __future__ import annotations

import logging
import os
import threading
from typing import Optional

from .transport import MQTTTransport
from ...core.classification.universal_classifier import UniversalClassifier
from ...core.classification.config_provider import HardcodedConfigProvider
from ...infrastructure.persistence.domain_storage_router import DomainStorageRouter
from ...infrastructure.audit.audit_logger import AuditLogger
from ...pipelines.resilience.deduplicator import MessageDeduplicator

logger = logging.getLogger(__name__)

# Singleton instance
_receiver_instance: Optional[UniversalMQTTReceiver] = None
_receiver_lock = threading.Lock()


class UniversalMQTTReceiver:
    """Receptor MQTT para ingesta universal.
    
    Suscribe a topics: {domain}/{source}/{stream}/data
    NO interfiere con el receiver IoT existente.
    """
    
    def __init__(self):
        """Inicializa el receptor universal."""
        self._started = False
        self._client = None
        self._thread = None
        
        # Componentes del pipeline
        self._transport = MQTTTransport()
        self._classifier = UniversalClassifier()
        self._config_provider = HardcodedConfigProvider()
        self._storage_router = DomainStorageRouter()
        self._audit_logger = AuditLogger()
        self._deduplicator = MessageDeduplicator(ttl_seconds=300)  # 5 min dedup
        
        # Estadísticas
        self._messages_processed = 0
        self._messages_rejected = 0
        self._messages_duplicated = 0
    
    def start(self) -> bool:
        """Inicia el receptor MQTT universal.
        
        Returns:
            True si se inició exitosamente
        """
        # Verificar feature flag
        if not os.getenv("FF_MQTT_UNIVERSAL", "false").lower() in ("true", "1", "yes", "on"):
            logger.info("[UniversalMQTT] Disabled (FF_MQTT_UNIVERSAL=false)")
            return False
        
        if self._started:
            logger.warning("[UniversalMQTT] Already started")
            return True
        
        try:
            # TODO: Implementar conexión MQTT real
            # import paho.mqtt.client as mqtt
            # self._client = mqtt.Client()
            # self._client.on_connect = self._on_connect
            # self._client.on_message = self._on_message
            # self._client.connect(host, port)
            # self._client.loop_start()
            
            self._started = True
            logger.info("[UniversalMQTT] Started successfully")
            return True
            
        except Exception as e:
            logger.exception(f"[UniversalMQTT] Failed to start: {e}")
            return False
    
    def stop(self) -> None:
        """Detiene el receptor MQTT universal."""
        if not self._started:
            return
        
        try:
            # Detener cliente MQTT
            if self._client:
                self._client.loop_stop()
                self._client.disconnect()
            
            self._started = False
            
            # Loguear estadísticas finales en audit
            logger.info(
                "[UniversalMQTT] Stopped - processed=%d rejected=%d duplicated=%d",
                self._messages_processed,
                self._messages_rejected,
                self._messages_duplicated,
            )
            
        except Exception as e:
            logger.exception(f"[UniversalMQTT] Error stopping: {e}")
    
    def _on_connect(self, client, userdata, flags, rc, properties=None):
        """Callback de conexión."""
        if rc == 0:
            logger.info("[UniversalMQTT] Connected to broker")
            
            # Suscribir a topics universales
            topics = [
                ("+/+/+/data", 1),  # {domain}/{source}/{stream}/data
            ]
            for topic, qos in topics:
                client.subscribe(topic, qos=qos)
                logger.info(f"[UniversalMQTT] Subscribed to {topic}")
        else:
            logger.error(f"[UniversalMQTT] Connection failed: rc={rc}")
    
    def _on_message(self, client, userdata, msg):
        """Callback de mensaje."""
        try:
            # 1. Parsear con MQTTTransport
            for data_point in self._transport.parse_message(msg):
                try:
                    # 2. Verificar que domain != 'iot' (rechazar)
                    if data_point.domain.lower() == 'iot':
                        logger.warning(
                            "[UniversalMQTT] Rejected domain='iot' - topic=%s",
                            msg.topic
                        )
                        self._messages_rejected += 1
                        continue
                    
                    # 3. Deduplicación
                    msg_id = f"{data_point.series_id}:{data_point.timestamp.isoformat()}"
                    if self._deduplicator.is_duplicate(msg_id):
                        logger.debug(
                            "[UniversalMQTT] Duplicate message - series_id=%s",
                            data_point.series_id
                        )
                        self._messages_duplicated += 1
                        continue
                    
                    # 4. Clasificar con UniversalClassifier
                    config = self._config_provider.get_config(data_point.series_id)
                    if config is None:
                        config = self._config_provider.get_default_config(data_point.domain)
                    
                    result = self._classifier.classify(data_point, config)
                    
                    # 5. Persistir con DomainStorageRouter
                    if result.should_persist:
                        success = self._storage_router.insert(data_point)
                        
                        if success:
                            self._messages_processed += 1
                            
                            # 6. Registrar en audit_logger
                            self._audit_logger.log_ingestion(
                                data_point=data_point,
                                classification=result.classification.value,
                                transport="mqtt",
                                status="accepted",
                            )
                            
                            logger.debug(
                                "[UniversalMQTT] Message processed - series_id=%s classification=%s",
                                data_point.series_id,
                                result.classification.value,
                            )
                        else:
                            self._messages_rejected += 1
                            logger.warning(
                                "[UniversalMQTT] Storage failed - series_id=%s",
                                data_point.series_id
                            )
                    else:
                        self._messages_rejected += 1
                        logger.info(
                            "[UniversalMQTT] Message rejected - series_id=%s reason=%s",
                            data_point.series_id,
                            result.reason,
                        )
                        
                        # Registrar rechazo en audit
                        self._audit_logger.log_ingestion(
                            data_point=data_point,
                            classification=result.classification.value,
                            transport="mqtt",
                            status="rejected",
                            error_message=result.reason,
                        )
                
                except Exception as e:
                    self._messages_rejected += 1
                    logger.exception(
                        "[UniversalMQTT] Error processing DataPoint: %s",
                        e
                    )
                    # Continuar con siguiente DataPoint (no crashear el receiver)
            
        except Exception as e:
            logger.exception(f"[UniversalMQTT] Error parsing message: {e}")


def get_universal_mqtt_receiver() -> Optional[UniversalMQTTReceiver]:
    """Obtiene la instancia singleton del receiver universal.
    
    Returns:
        UniversalMQTTReceiver si está habilitado, None si no
    """
    global _receiver_instance
    
    # Verificar feature flag
    if not os.getenv("FF_MQTT_UNIVERSAL", "false").lower() in ("true", "1", "yes", "on"):
        return None
    
    with _receiver_lock:
        if _receiver_instance is None:
            _receiver_instance = UniversalMQTTReceiver()
        return _receiver_instance


def start_universal_mqtt() -> bool:
    """Inicia el receptor MQTT universal.
    
    Returns:
        True si se inició exitosamente
    """
    receiver = get_universal_mqtt_receiver()
    if receiver:
        return receiver.start()
    return False


def stop_universal_mqtt() -> None:
    """Detiene el receptor MQTT universal."""
    receiver = get_universal_mqtt_receiver()
    if receiver:
        receiver.stop()

"""HTTP Endpoints - Endpoints REST para ingesta universal.

IMPORTANTE: Este archivo NO modifica endpoints IoT existentes.
Define endpoints NUEVOS para ingesta universal (dominios no-IoT).
"""

from __future__ import annotations

import logging
from typing import Dict

from fastapi import APIRouter, Depends, Header, HTTPException, status

from .schemas import DataPacketIn, DataIngestResult
from .transport import HTTPTransport
from ...core.classification.universal_classifier import UniversalClassifier
from ...core.classification.config_provider import HardcodedConfigProvider
from ...core.domain.classification import DataPointClass
from ...auth.api_key_validator import verify_api_key, verify_source_access
from ...auth.authorization import ApiKeyInfo

logger = logging.getLogger(__name__)

router = APIRouter(tags=["universal-ingest"])

# Inicializar componentes
_transport = HTTPTransport()
_classifier = UniversalClassifier()
_config_provider = HardcodedConfigProvider()


# verify_api_key ahora importado desde auth.api_key_validator


@router.post("/ingest/data", response_model=DataIngestResult)
async def ingest_data(
    packet: DataPacketIn,
    api_key_info: ApiKeyInfo = Depends(verify_api_key),
) -> DataIngestResult:
    """Endpoint para ingesta de datos universal (dominios no-IoT).
    
    Args:
        packet: Paquete de datos con domain, source_id y data_points
        api_key_info: Información del API key validado
        
    Returns:
        Resultado de la ingesta con conteos y clasificaciones
        
    Raises:
        HTTPException: Si domain='iot' o hay errores de validación o autorización
    """
    # domain='iot' ya está bloqueado por el validator de DataPacketIn
    # pero agregamos verificación adicional por seguridad
    if packet.domain.lower() == "iot":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="domain='iot' not allowed. Use /ingest/packets endpoint instead.",
        )
    
    # Verificar autorización para source_id y domain
    verify_source_access(api_key_info, packet.source_id, packet.domain)
    
    accepted = 0
    rejected = 0
    classifications: Dict[str, int] = {}
    errors = []
    
    try:
        # Parsear mensaje a DataPoints
        for data_point in _transport.parse_message(packet):
            try:
                # Obtener configuración
                config = _config_provider.get_config(data_point.series_id)
                if config is None:
                    config = _config_provider.get_default_config(data_point.domain)
                
                # Clasificar
                result = _classifier.classify(data_point, config)
                
                # Contabilizar clasificación
                class_name = result.classification.value
                classifications[class_name] = classifications.get(class_name, 0) + 1
                
                # Verificar si debe persistirse
                if result.should_persist:
                    accepted += 1
                    
                    # TODO TAREA-8: Persistir en PostgreSQL
                    # domain_router.insert(data_point)
                    
                    # TODO: Publicar a ML si es NORMAL
                    # if result.should_publish_to_ml:
                    #     broker.publish(data_point.to_ml_format())
                    
                    # TODO: Crear alerta si es necesario
                    # if result.should_alert:
                    #     alert_service.create_alert(data_point, result)
                    
                    logger.debug(
                        f"DataPoint accepted: series_id={data_point.series_id} "
                        f"value={data_point.value} classification={class_name}"
                    )
                else:
                    rejected += 1
                    logger.warning(
                        f"DataPoint rejected: series_id={data_point.series_id} "
                        f"reason={result.reason}"
                    )
                
            except Exception as e:
                rejected += 1
                error_msg = f"Error processing DataPoint: {str(e)}"
                errors.append(error_msg)
                logger.exception(error_msg)
        
        return DataIngestResult(
            accepted=accepted,
            rejected=rejected,
            classifications=classifications,
            errors=errors,
        )
        
    except Exception as e:
        logger.exception(f"Error in /ingest/data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("/ingest/health")
async def health_check() -> Dict[str, str]:
    """Health check para ingesta universal.
    
    Returns:
        Estado del servicio
    """
    # TODO TAREA-8: Verificar disponibilidad de PostgreSQL
    return {
        "status": "ok",
        "transport": "http",
        "classifier": "universal",
    }

"""Core module - Arquitectura modular de ingesta IoT.

Estructura:
- transport/   → Recepción MQTT
- domain/      → Modelos y contratos de dominio
- adapters/    → Adaptadores de contrato
- pipeline/    → Procesamiento de lecturas
- validation/  → Validación de datos
- redis/       → Publicación a Redis Streams
- monitoring/  → Métricas y observabilidad
"""

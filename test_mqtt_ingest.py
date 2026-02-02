"""Test del flujo MQTT de ingesta - diagnóstico completo."""

import os
import sys
from pathlib import Path

# Cargar .env
from dotenv import load_dotenv
env_file = Path(__file__).parent / ".env"
if env_file.exists():
    load_dotenv(env_file)
    print(f"[OK] .env cargado desde: {env_file}")
else:
    print(f"[ERROR] .env NO existe en: {env_file}")

print("\n=== VARIABLES DE ENTORNO ===")
ff_mqtt = os.getenv("FF_MQTT_INGEST_ENABLED", "NOT_SET")
broker_host = os.getenv("MQTT_BROKER_HOST", "NOT_SET")
broker_port = os.getenv("MQTT_BROKER_PORT", "NOT_SET")
print(f"FF_MQTT_INGEST_ENABLED = {ff_mqtt}")
print(f"MQTT_BROKER_HOST = {broker_host}")
print(f"MQTT_BROKER_PORT = {broker_port}")

if ff_mqtt.lower() != "true":
    print("\n[ERROR CRÍTICO] FF_MQTT_INGEST_ENABLED != true")
    sys.exit(1)

# Verificar que iot_mqtt esté disponible
print("\n=== VERIFICANDO iot_mqtt ===")
try:
    # Agregar path del proyecto
    project_root = Path(__file__).parent.parent
    sys.path.insert(0, str(project_root))
    
    from iot_mqtt import MQTTClient, MQTTSubscriber, MQTTConfig
    from iot_mqtt.config import get_feature_flags, get_mqtt_config
    print("[OK] iot_mqtt importado correctamente")
    
    # Verificar feature flags
    flags = get_feature_flags()
    print(f"\n=== FEATURE FLAGS ===")
    print(f"mqtt_ingest_enabled: {flags.mqtt_ingest_enabled}")
    print(f"mqtt_ingest_dual_mode: {flags.mqtt_ingest_dual_mode}")
    
    # Verificar config
    config = get_mqtt_config()
    print(f"\n=== MQTT CONFIG ===")
    print(f"broker_host: {config.broker_host}")
    print(f"broker_port: {config.broker_port}")
    
except ImportError as e:
    print(f"[ERROR] No se pudo importar iot_mqtt: {e}")
    print("\nEsto significa que el módulo iot_mqtt no está en el PYTHONPATH")
    print("El receptor MQTT NO funcionará")
    sys.exit(1)

# Verificar mqtt_receiver
print("\n=== VERIFICANDO mqtt_receiver ===")
try:
    from ingest_api.mqtt import MQTTIngestReceiver, MQTTRedisBridge
    print("[OK] mqtt_receiver importado correctamente")
except ImportError as e:
    print(f"[ERROR] No se pudo importar mqtt_receiver: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*60)
print("RESULTADO:")
print("="*60)
print("""
Si todos los imports fueron exitosos, el problema puede ser:
1. El servicio de ingesta no está corriendo
2. El servicio no cargó el .env correctamente
3. EMQX no está corriendo

Para probar el flujo completo:
1. Iniciar EMQX: docker-compose -f iot_mqtt/docker-compose.yml up -d
2. Iniciar ingesta: uvicorn ingest_api.main:app --port 8001
3. Verificar health: curl http://localhost:8001/mqtt/health
4. Enviar mensaje de prueba desde simulador
""")

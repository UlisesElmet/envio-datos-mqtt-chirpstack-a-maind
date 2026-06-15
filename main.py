import json
import logging
import os

import paho.mqtt.client as mqtt
import pymysql
import pytz
import requests
from datetime import datetime
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuración (vía variables de entorno / .env)
# ---------------------------------------------------------------------------
load_dotenv()

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")

ENDPOINT_URL = os.getenv(
    "ENDPOINT_URL", "https://apis.maind.cl/api/reading/webhook_dispositivos"
)

DB_HOST = os.getenv("DB_HOST", "10.0.3.147")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_NAME = os.getenv("DB_NAME", "")

TOPIC = f"application/+/device/+/event/up"

# Perfiles de dispositivo cuyos mensajes procesamos (separados por coma en .env)
ALLOWED_PROFILES = {
    p.strip()
    for p in os.getenv(
        "ALLOWED_PROFILES",
        "RADIO_LIB,Sense V1,SENSEV2_TEST,REAID MUV V1 - ABP,IRIS_V1_AC",
    ).split(",")
    if p.strip()
}

CHILE_TZ = pytz.timezone("America/Santiago")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mqtt_bridge")


# ---------------------------------------------------------------------------
# Base de datos: conexión perezosa + caché nombre -> id
# ---------------------------------------------------------------------------
_conn = None
_device_cache = {}  # nombre (str) -> id (int)


def get_conn():
    """Devuelve una conexión MariaDB viva, reconectando si es necesario."""
    global _conn
    if _conn is None:
        _conn = pymysql.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            autocommit=True,
            connect_timeout=5,
        )
    _conn.ping(reconnect=True)
    return _conn


def get_device_id(nombre):
    """Resuelve el id del dispositivo a partir de su nombre.

    Usa una caché en memoria; ante un miss consulta MariaDB y, si lo encuentra,
    lo guarda en la caché. Devuelve None si el dispositivo no existe.
    """
    if not nombre:
        return None
    if nombre in _device_cache:
        return _device_cache[nombre]

    with get_conn().cursor() as cur:
        cur.execute(
            "SELECT id FROM dispositivos WHERE nombre = %s LIMIT 1", (nombre,)
        )
        row = cur.fetchone()

    if row:
        _device_cache[nombre] = row[0]
        return row[0]
    return None


# ---------------------------------------------------------------------------
# Sesión HTTP reutilizable con reintentos
# ---------------------------------------------------------------------------
def build_http_session():
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=frozenset(["POST"]),
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


http = build_http_session()


# ---------------------------------------------------------------------------
# Callbacks MQTT
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        log.info("Conectado al broker MQTT. Suscribiendo a los tópicos...")
        client.subscribe(TOPIC)
        log.info("Suscrito a %s", TOPIC)
    else:
        log.error("Fallo al conectar al broker MQTT (código %s)", reason_code)


def on_message(client, userdata, msg):
    log.info("Mensaje recibido en %s", msg.topic)
    try:
        data = json.loads(msg.payload.decode())

        device_profile = data.get("deviceProfileName")
        if device_profile not in ALLOWED_PROFILES:
            return

        decoded_payload = json.loads(data.get("objectJSON"))

        # Parámetros tal como llegan en el payload (sin renombrar).
        parametros = [
            {"parametro": clave, "valor": valor}
            for clave, valor in decoded_payload.items()
        ]

        if not (parametros or device_profile == "IRIS_V1_AC"):
            return

        # Resolver el id real del dispositivo desde la base de datos.
        device_name = data.get("deviceName")
        device_id = get_device_id(device_name)
        if device_id is None:
            log.warning(
                "Dispositivo '%s' no encontrado en la tabla 'dispositivos'; "
                "se omite el mensaje.",
                device_name,
            )
            return

        rx_info = data.get("rxInfo") or []
        if rx_info:
            parametros.append({"parametro": "RSSI", "valor": rx_info[0].get("rssi")})
            parametros.append({"parametro": "SNR", "valor": rx_info[0].get("loRaSNR")})

        datos = {
            "server_received_at": datetime.now(CHILE_TZ).isoformat(),
            "device_id": device_id,
            "parametros": parametros,
        }
        log.info("Enviando datos: %s", datos)

        response = http.post(ENDPOINT_URL, json=datos, timeout=10)
        log.info("Datos enviados: %s", response.status_code)
    except Exception as e:
        log.error("Error al procesar/enviar los datos: %s", e)


# ---------------------------------------------------------------------------
# Arranque del cliente MQTT
# ---------------------------------------------------------------------------
def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.reconnect_delay_set(min_delay=1, max_delay=60)
    client.connect(MQTT_BROKER, MQTT_PORT)
    log.info("Escuchando mensajes MQTT...")
    client.loop_forever()


if __name__ == "__main__":
    main()

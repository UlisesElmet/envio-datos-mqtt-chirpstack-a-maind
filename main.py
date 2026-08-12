import json
import logging
import os

import paho.mqtt.client as mqtt
import pymysql
import pytz
import requests
import yaml
from datetime import datetime
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mqtt_bridge")

# ---------------------------------------------------------------------------
# Configuración (vía config.yaml)
# ---------------------------------------------------------------------------
CONFIG_FILE = Path(os.getenv("CONFIG_FILE", "config.yaml"))


def load_config(path):
    """Carga la configuración desde un archivo YAML.

    Si el archivo no existe se registra una advertencia y se usan los valores
    por defecto definidos más abajo.
    """
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    log.warning("No se encontró %s; se usan los valores por defecto.", path)
    return {}


config = load_config(CONFIG_FILE)

mqtt_cfg = config.get("mqtt") or {}
MQTT_BROKER = mqtt_cfg.get("broker", "localhost")
MQTT_PORT = int(mqtt_cfg.get("port", 1883))
MQTT_USERNAME = mqtt_cfg.get("username", "")
MQTT_PASSWORD = mqtt_cfg.get("password", "")

endpoint_cfg = config.get("endpoint") or {}
ENDPOINT_URL = endpoint_cfg.get(
    "url", "https://apis.maind.cl/api/reading/webhook_dispositivos"
)

db_cfg = config.get("database") or {}
DB_HOST = db_cfg.get("host", "10.0.3.147")
DB_PORT = int(db_cfg.get("port", 3306))
DB_USER = db_cfg.get("user", "")
DB_PASSWORD = db_cfg.get("password", "")
DB_NAME = db_cfg.get("name", "")

SAVE_DATA = config.get("save_data", False)
# Carpeta del buffer de envío. Configurable con `save_dir`: en Docker el
# merge la apunta a /data/save_data (volumen persistente: los pendientes
# sobreviven recreaciones del contenedor); sin config queda el clásico
# ./save_data relativo (modo nativo).
SAVE_DIR = Path(config.get("save_dir", "save_data"))
SAVE_DIR.mkdir(parents=True, exist_ok=True)
TOPIC = f"application/+/device/+/event/up"

# Origen del id de dispositivo: "database" (consulta a MariaDB) o "config"
# (mapa nombre -> id definido en config.yaml). Si no se declara, se decide
# solo con prioridad BD > config: hay bloque `database` con host -> database;
# si no -> config.
_default_source = "database" if db_cfg.get("host") else "config"
DEVICE_ID_SOURCE = (config.get("device_id_source") or _default_source).strip().lower()

# Mapa nombre de dispositivo -> id, usado cuando DEVICE_ID_SOURCE == "config".
DEVICES_MAP = {
    str(nombre): int(device_id)
    for nombre, device_id in (config.get("devices") or {}).items()
}

# Perfiles de dispositivo cuyos mensajes procesamos.
ALLOWED_PROFILES = {
    p.strip()
    for p in config.get(
        "allowed_profiles",
        ["RADIO_LIB", "Sense V1", "SENSEV2_TEST", "REAID MUV V1 - ABP", "IRIS_V1_AC"],
    )
    if p and p.strip()
}

CHILE_TZ = pytz.timezone("America/Santiago")


# ---------------------------------------------------------------------------
# Base de datos: conexión perezosa + caché nombre -> id
# ---------------------------------------------------------------------------
_conn = None
_device_cache = {}  # nombre (str) -> id (int)

def guardar_datos(datos, device_id, server_received_at):
    nombre_archivo = f"{device_id}_{server_received_at.strftime('%Y%m%d_%H%M%S')}.json"
    path = SAVE_DIR / nombre_archivo

    with open(path, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=4)

    return path

def postear_archivos_pendientes(http, endpoint_url):
    for path in sorted(SAVE_DIR.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                datos = json.load(f)

            log.info("Enviando archivo pendiente: %s", path)

            response = http.post(endpoint_url, json=datos, timeout=10)

            if response.ok:
                log.info("Archivo enviado correctamente, eliminando: %s", path)
                path.unlink()
            else:
                log.warning(
                    "Error HTTP %s enviando %s: %s",
                    response.status_code,
                    path,
                    response.text,
                )

                # Si NO es timeout ni problema de internet, se borra
                path.unlink()

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning("Sin conexión o timeout enviando %s: %s", path, e)
            # No borrar archivo

        except Exception as e:
            log.exception("Error no recuperable con archivo %s: %s", path, e)
            # Error distinto a timeout/conexión: borrar archivo
            path.unlink(missing_ok=True)

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

    Según ``DEVICE_ID_SOURCE`` el id se obtiene del mapa definido en
    ``config.yaml`` (``config``) o consultando MariaDB (``database``). En el
    caso de la base de datos usa una caché en memoria para evitar consultas
    repetidas. Devuelve ``None`` si el dispositivo no existe.
    """
    if not nombre:
        return None

    if DEVICE_ID_SOURCE == "config":
        return DEVICES_MAP.get(nombre)

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

        # ChirpStack v4 anida la info del dispositivo en "deviceInfo";
        # v3 la trae en la raíz. Se soportan ambos.
        device_info = data.get("deviceInfo") or {}
        device_profile = (
            device_info.get("deviceProfileName")
            or data.get("deviceProfileName")
        )

        # v4 entrega el payload decodificado en "object" (ya es un dict);
        # v3 lo entrega en "objectJSON" (string JSON).
        object_json = data.get("object")
        if object_json is None:
            object_json = json.loads(data.get("objectJSON", "{}"))

        decoded_payload = (
            object_json.get("decode")
            or object_json.get("data")
            or object_json
        )

        # ACK de comandos (p. ej. REAID SWITCH V1): el dispositivo confirma un
        # comando con ACK/command_name/command_value. Se publica retenido en
        # {device_id}/ack y no se envía al endpoint.
        ack = decoded_payload.get("ACK", data.get("ACK"))
        if ack is not None:
            device_name = device_info.get("deviceName") or data.get("deviceName")
            device_id = get_device_id(device_name)
            if device_id is None:
                log.warning(
                    "Dispositivo '%s' no encontrado; se omite el ACK.",
                    device_name,
                )
                return

            command_name = decoded_payload.get(
                "command_name", data.get("command_name")
            )
            command_value = decoded_payload.get(
                "command_value", data.get("command_value")
            )
            ack_payload = {
                "server_received_at": datetime.now(CHILE_TZ).isoformat(),
                "ACK": ack,
            }
            if command_name is not None:
                ack_payload[str(command_name)] = command_value

            client.publish(
                f"{device_id}/ack",
                json.dumps(ack_payload, ensure_ascii=False),
                retain=True,
            )
            log.info("ACK publicado en %s/ack: %s", device_id, ack_payload)
            return

        if device_profile not in ALLOWED_PROFILES:
            return

        # Parámetros tal como llegan en el payload (sin renombrar).
        parametros = [
            {"parametro": clave, "valor": valor}
            for clave, valor in decoded_payload.items()
        ]

        if not (parametros or device_profile == "IRIS_V1_AC"):
            return

        # Resolver el id real del dispositivo desde la base de datos.
        device_name = device_info.get("deviceName") or data.get("deviceName")
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
            # v4 usa "snr"; v3 usaba "loRaSNR".
            parametros.append(
                {
                    "parametro": "SNR",
                    "valor": rx_info[0].get("snr", rx_info[0].get("loRaSNR")),
                }
            )

        server_received_at = datetime.now(CHILE_TZ)
        datos = {
            "server_received_at": server_received_at.isoformat(),
            "device_id": device_id,
            "parametros": parametros,
        }

        # Publica el último JSON en {device_id}/last (retenido) para que el
        # topic conserve siempre el valor más reciente del sensor.
        client.publish(
            f"{device_id}/last",
            json.dumps(datos, ensure_ascii=False),
            retain=True,
        )

        if SAVE_DATA:
            guardar_datos(datos, device_id, server_received_at)
            postear_archivos_pendientes(http, ENDPOINT_URL)
        else:
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

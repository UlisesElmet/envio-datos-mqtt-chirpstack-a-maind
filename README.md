# Puente MQTT ChirpStack → Webhook

Servicio en Python que escucha eventos de uplink de [ChirpStack](https://www.chirpstack.io/) por MQTT, filtra los mensajes según el perfil de dispositivo, resuelve el `id` interno del dispositivo en MariaDB y reenvía los datos normalizados a un endpoint HTTP (webhook).

## Tabla de contenidos

- [Arquitectura](#arquitectura)
- [Flujo de procesamiento](#flujo-de-procesamiento)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)
- [Detalle del código](#detalle-del-código)
- [Formato de los datos enviados](#formato-de-los-datos-enviados)
- [Base de datos](#base-de-datos)
- [Manejo de errores y resiliencia](#manejo-de-errores-y-resiliencia)

## Arquitectura

```
ChirpStack ──(MQTT uplink)──▶ main.py ──(consulta id)──▶ MariaDB
                                 │
                                 └──(HTTP POST JSON)──▶ Webhook (ENDPOINT_URL)
```

El servicio actúa como un **puente (bridge)**: consume mensajes del broker MQTT, los transforma y los publica hacia una API web. Se apoya en MariaDB únicamente para traducir el nombre del dispositivo a su `id` interno.

## Flujo de procesamiento

1. Se conecta al broker MQTT y se suscribe al tópico de uplinks: `application/+/device/+/event/up`.
2. Por cada mensaje recibido (`on_message`):
   1. Decodifica el payload JSON.
   2. Descarta el mensaje si el `deviceProfileName` no está en `ALLOWED_PROFILES`.
   3. Extrae los parámetros desde `objectJSON` (payload decodificado por ChirpStack).
   4. Resuelve el `device_id` consultando la tabla `dispositivos` por nombre (con caché en memoria). Si no existe, descarta el mensaje.
   5. Agrega los parámetros de radio `RSSI` y `SNR` si vienen en `rxInfo`.
   6. Arma el objeto final con marca de tiempo en zona horaria de Chile y lo envía vía `HTTP POST` al `ENDPOINT_URL`.

## Requisitos

- Python 3.8+
- Acceso a un broker MQTT de ChirpStack
- Acceso a una base de datos MariaDB/MySQL con la tabla `dispositivos`
- Acceso de red al endpoint del webhook

Dependencias (ver [requirements.txt](requirements.txt)):

| Paquete | Uso |
|---|---|
| `paho-mqtt>=2.0` | Cliente MQTT (API v2) |
| `requests>=2.31` | Envío HTTP con reintentos |
| `pytz` | Zona horaria `America/Santiago` |
| `PyMySQL>=1.1` | Conexión a MariaDB |
| `python-dotenv>=1.0` | Carga de variables desde `.env` |

## Instalación

```bash
# Crear y activar un entorno virtual (opcional pero recomendado)
python -m venv venv
venv\Scripts\activate        # Windows
# source venv/bin/activate   # Linux / macOS

# Instalar dependencias
pip install -r requirements.txt
```

## Configuración

La configuración se realiza mediante variables de entorno, que pueden definirse en un archivo `.env` (cargado automáticamente con `python-dotenv`). Copia [.env.example](.env.example) a `.env` y ajusta los valores.

| Variable | Por defecto | Descripción |
|---|---|---|
| `MQTT_BROKER` | `localhost` | Host del broker MQTT |
| `MQTT_PORT` | `1883` | Puerto del broker MQTT |
| `MQTT_USERNAME` | *(vacío)* | Usuario MQTT |
| `MQTT_PASSWORD` | *(vacío)* | Contraseña MQTT |
| `ENDPOINT_URL` | `https://apis.maind.cl/api/reading/webhook_dispositivos` | URL del webhook destino |
| `DB_HOST` | `10.0.3.147` | Host de MariaDB |
| `DB_PORT` | `3306` | Puerto de MariaDB |
| `DB_USER` | *(vacío)* | Usuario de la base de datos |
| `DB_PASSWORD` | *(vacío)* | Contraseña de la base de datos |
| `DB_NAME` | *(vacío)* | Nombre de la base de datos |
| `ALLOWED_PROFILES` | `RADIO_LIB,Sense V1,SENSEV2_TEST,REAID MUV V1 - ABP,IRIS_V1_AC` | Perfiles de dispositivo a procesar (separados por coma) |

> **Nota:** la variable `APP_IDS` aparece en `.env.example` pero actualmente **no se usa** en el código; la suscripción emplea el comodín `+` para todas las aplicaciones.

## Ejecución

```bash
python main.py
```

El proceso se mantiene en ejecución indefinidamente (`loop_forever`), reconectándose automáticamente al broker ante caídas (reintento entre 1 y 60 segundos).

Los registros se emiten a la consola en formato:

```
2026-06-15 12:00:00,000 [INFO] Mensaje recibido en application/8/device/abc/event/up
```

## Detalle del código

Todo el servicio reside en [main.py](main.py).

### Configuración (`load_dotenv` y variables globales)

Carga las variables de entorno y define el tópico de suscripción y el conjunto de perfiles permitidos. El tópico es:

```
application/+/device/+/event/up
```

donde los `+` son comodines de un nivel para el `id` de aplicación y el `id` de dispositivo respectivamente.

### `get_conn()`

Devuelve una conexión a MariaDB con conexión perezosa (se crea en el primer uso) y reconexión automática vía `ping(reconnect=True)`. Usa `autocommit=True` y `connect_timeout=5`.

### `get_device_id(nombre)`

Traduce el nombre del dispositivo a su `id` interno consultando la tabla `dispositivos`. Mantiene una **caché en memoria** (`_device_cache`) para evitar consultas repetidas. Devuelve `None` si el dispositivo no existe.

### `build_http_session()`

Crea una sesión `requests` reutilizable con política de reintentos:

- Hasta **3 reintentos**
- `backoff_factor=0.5` (espera incremental)
- Reintenta ante códigos `500, 502, 503, 504`
- Solo para el método `POST`

### `on_connect()`

Callback al conectar con el broker. Si la conexión es exitosa (`reason_code == 0`), se suscribe al tópico; en caso contrario registra el error.

### `on_message()`

Núcleo del procesamiento. Recibe, filtra, transforma y reenvía cada mensaje (ver [Flujo de procesamiento](#flujo-de-procesamiento)). Captura cualquier excepción para que un mensaje malformado no detenga el servicio.

### `main()`

Inicializa el cliente MQTT (API v2), registra los callbacks, configura credenciales y la política de reconexión, conecta y entra en el bucle infinito de escucha.

## Formato de los datos enviados

El cuerpo del `POST` al webhook tiene esta estructura:

```json
{
  "server_received_at": "2026-06-15T12:00:00.000000-04:00",
  "device_id": 123,
  "parametros": [
    { "parametro": "temperatura", "valor": 22.5 },
    { "parametro": "humedad", "valor": 60 },
    { "parametro": "RSSI", "valor": -85 },
    { "parametro": "SNR", "valor": 9.5 }
  ]
}
```

- `server_received_at`: marca de tiempo del servidor en zona horaria `America/Santiago`, en formato ISO 8601.
- `device_id`: `id` interno resuelto desde MariaDB.
- `parametros`: lista de pares `parametro`/`valor`. Incluye todas las claves del payload decodificado (`objectJSON`) más `RSSI` y `SNR` cuando están disponibles en `rxInfo`.

## Base de datos

El servicio solo lee de la tabla `dispositivos`. Se asume una estructura mínima:

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | entero | Identificador interno del dispositivo |
| `nombre` | texto | Nombre que coincide con `deviceName` de ChirpStack |

Consulta utilizada:

```sql
SELECT id FROM dispositivos WHERE nombre = %s LIMIT 1;
```

## Manejo de errores y resiliencia

- **MQTT:** reconexión automática con backoff (1–60 s).
- **MariaDB:** reconexión vía `ping(reconnect=True)` en cada uso.
- **HTTP:** hasta 3 reintentos con backoff ante errores 5xx; `timeout=10 s`.
- **Procesamiento de mensajes:** cada mensaje se procesa dentro de un `try/except`; un error puntual se registra pero no interrumpe el servicio.
- **Caché de dispositivos:** se mantiene mientras el proceso vive; reiniciar el servicio la limpia.

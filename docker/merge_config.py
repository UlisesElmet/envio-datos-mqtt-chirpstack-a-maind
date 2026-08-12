#!/usr/bin/env python3
"""
merge_config.py — Genera el config.yaml final de cada servicio a partir de:

  1. common.yaml   : configuración COMPARTIDA por todos los servicios del equipo
                     (broker MQTT, base de datos, URL base de las APIs, datos del sitio).
  2. override.yaml : configuración ESPECÍFICA del servicio (islas Modbus, token
                     de ChirpStack, perfiles permitidos, etc.).

El script conoce el "esquema" que espera cada servicio y traduce los valores
comunes a las claves que cada script ya lee — así NO hay que modificar la
lógica de los servicios. El override se aplica al final con deep-merge, por lo
que cualquier clave puede sobrescribirse por sitio si hiciera falta.

Dentro del override se pueden referenciar valores comunes con la sintaxis:
    "${common:mqtt.host}"   "${common:database.user}"   "${env:MI_VARIABLE}"

Uso:
    merge_config.py --service {mqtt-bridge|modbus-driver|chirp-scheduler}
                    --common /config/common.yaml
                    --override /config/override.yaml
                    --out /app/config.yaml
"""
import argparse
import os
import re
import sys

import yaml

_SUBST_RE = re.compile(r"^\$\{(common|env):([^}]+)\}$")


def load_yaml(path, required=False):
    if not os.path.exists(path):
        if required:
            sys.exit(f"[merge_config] ERROR: no existe {path}")
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def dig(data, dotted, default=None):
    cur = data
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def deep_merge(base, override):
    """override gana; los dicts se combinan recursivamente."""
    if not isinstance(base, dict) or not isinstance(override, dict):
        return override
    merged = dict(base)
    for key, value in override.items():
        if key in merged:
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def substitute(node, common):
    """Reemplaza strings ${common:a.b} / ${env:VAR} en cualquier nivel."""
    if isinstance(node, dict):
        return {k: substitute(v, common) for k, v in node.items()}
    if isinstance(node, list):
        return [substitute(v, common) for v in node]
    if isinstance(node, str):
        m = _SUBST_RE.match(node.strip())
        if m:
            kind, key = m.groups()
            if kind == "common":
                value = dig(common, key)
                if value is None:
                    sys.exit(f"[merge_config] ERROR: '{key}' no existe en common.yaml "
                             f"(referenciado como ${{common:{key}}})")
                return value
            return os.environ.get(key, "")
    return node


# ---------------------------------------------------------------------------
# Esquema base por servicio, construido desde common.yaml
# ---------------------------------------------------------------------------

def _db_block(db):
    """Bloque database normalizado. None si el común no define BD (sin host)."""
    if not db or not db.get("host"):
        return None
    return {
        "host": db.get("host"),
        "port": db.get("port", 3306),
        "user": db.get("user", ""),
        "password": db.get("password", ""),
        "name": db.get("name", ""),
    }


def base_mqtt_bridge(c):
    """envio-datos-mqtt-chirpstack-a-maind: espera mqtt.broker (no .host).

    `device_id_source` NO se fuerza: el servicio decide solo con prioridad
    BD > config (si hay bloque database usa la BD; si no, el mapa `devices`
    del override).
    """
    mqtt = c.get("mqtt", {})
    api = c.get("api_base", "https://apis.maind.cl").rstrip("/")
    base = {
        "mqtt": {
            "broker": mqtt.get("host", "localhost"),
            "port": mqtt.get("port", 1883),
            "username": mqtt.get("username", ""),
            "password": mqtt.get("password", ""),
        },
        "endpoint": {"url": f"{api}/api/reading/webhook_dispositivos"},
        # Buffer-and-forward en disco: cada payload se guarda antes de postear
        # y se borra al confirmar; si no hay internet queda pendiente y se
        # reenvía solo. En /data para que sobreviva recreaciones (volumen).
        "save_data": True,
        "save_dir": "/data/save_data",
    }
    db = _db_block(c.get("database"))
    if db:
        base["database"] = db
    return base


def base_modbus_driver(c):
    """modbus-driver: site + URLs desde common; directorios apuntando a /data
    (el volumen persistente del contenedor).

    El bloque `mqtt:` (broker para la programación condicional) se agrega
    SOLO si el common lo declara (tiene host); si no, se omite y el driver
    arranca sin programación condicional.
    """
    api = c.get("api_base", "https://apis.maind.cl").rstrip("/")
    base = {
        "site": c.get("site", {}),
        "URLs": {
            # Webhook UNIFICADO: la API recibe AR y HVAC en el mismo endpoint
            # (webhook_dispositivos, el mismo que usan mqtt-bridge y
            # health-check). Antes eran webhook_ar / webhook_hvac separados.
            "ar_api_url": f"{api}/api/reading/webhook_dispositivos",
            "hvac_api_url": f"{api}/api/reading/webhook_dispositivos",
            "schedule_log_url": f"{api}/api/scheduler/webhook/log",
            "set_ocupado_url": f"{api}/api/hvac/set-ocupado",
        },
        "directories": {
            "pendings_posts_directory_ar": "/data/pendings_posts_ar",
            "pendings_posts_directory_hvac": "/data/pendings_posts_hvac",
            "pendings_posts_directory_alerts_minipc": "/data/alertas_minipc",
            "failed_posts_directory": "/data/failed_posts",
            "successful_posts_directory": "/data/successful_posts",
            "schedule_path": "/data/horario",
        },
    }
    # Broker MQTT (programación condicional): solo si el common lo declara.
    # El driver toma de aquí host/credenciales; topic y max_age_minutes usan
    # sus defaults y pueden ajustarse en el override del sitio.
    mqtt = c.get("mqtt", {})
    if mqtt.get("host"):
        base["mqtt"] = {
            "host": mqtt.get("host"),
            "port": mqtt.get("port", 1883),
            "username": mqtt.get("username", ""),
            "password": mqtt.get("password", ""),
        }
    return base


def base_chirp_scheduler(c):
    """ProgramacionHorarioChirpstack: mqtt y database en nivel superior.

    `devices`/`sensors` no declaran `source`: el scheduler decide solo con
    prioridad BD > MQTT > config. Con el bloque `database:` presente, ambos
    usan la BD; sin BD, `sensors` cae al broker MQTT y `devices` al mapa del
    override.
    """
    mqtt = c.get("mqtt", {})
    base = {
        "mqtt": {
            "enabled": False,  # el override lo activa si el sitio usa ACK
            "host": mqtt.get("host", "localhost"),
            "port": mqtt.get("port", 1883),
            "username": mqtt.get("username", ""),
            "password": mqtt.get("password", ""),
        },
    }
    db = _db_block(c.get("database"))
    if db:
        base["database"] = db
    return base


BUILDERS = {
    "mqtt-bridge": base_mqtt_bridge,
    "modbus-driver": base_modbus_driver,
    "chirp-scheduler": base_chirp_scheduler,
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True, choices=sorted(BUILDERS))
    parser.add_argument("--common", required=True)
    parser.add_argument("--override", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    common = load_yaml(args.common, required=True)
    override = load_yaml(args.override, required=False)

    config = BUILDERS[args.service](common)
    config = deep_merge(config, substitute(override, common))

    with open(args.out, "w", encoding="utf-8") as f:
        f.write("# ARCHIVO GENERADO por merge_config.py — no editar a mano.\n"
                "# Edita /config/common.yaml (compartido) o el override del servicio.\n")
        yaml.safe_dump(config, f, allow_unicode=True, sort_keys=False)
    print(f"[merge_config] {args.service}: config generado en {args.out}")


if __name__ == "__main__":
    main()

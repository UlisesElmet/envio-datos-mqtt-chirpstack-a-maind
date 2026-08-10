#!/bin/sh
# Entrypoint común: si hay config compartida montada, genera el config.yaml
# final del servicio antes de arrancar. Si no hay common.yaml montado, el
# servicio arranca con el config.yaml que traiga (modo compatible).
set -e

COMMON="${COMMON_CONFIG:-/config/common.yaml}"
OVERRIDE="${OVERRIDE_CONFIG:-/config/override.yaml}"
OUT="${MERGED_CONFIG:-/app/config.yaml}"

if [ -f "$COMMON" ]; then
    python /opt/maind/merge_config.py \
        --service "${MAIND_SERVICE:?falta MAIND_SERVICE}" \
        --common "$COMMON" \
        --override "$OVERRIDE" \
        --out "$OUT"
else
    echo "[entrypoint] Aviso: no se encontró $COMMON; se usa el config.yaml existente."
fi

exec "$@"

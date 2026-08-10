# ---------------------------------------------------------------------------
# Puente MQTT ChirpStack -> Webhook
# Imagen final: ~80 MB comprimida. Sin build stage porque no compila nada.
# ---------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    TZ=America/Santiago \
    MAIND_SERVICE=mqtt-bridge \
    CONFIG_FILE=/app/config.yaml

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Herramientas comunes de arranque (merge de config compartida)
COPY docker/entrypoint.sh docker/merge_config.py /opt/maind/
RUN chmod +x /opt/maind/entrypoint.sh

COPY main.py .

ENTRYPOINT ["/opt/maind/entrypoint.sh"]
CMD ["python", "main.py"]

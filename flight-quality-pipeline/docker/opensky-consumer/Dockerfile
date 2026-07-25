# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY docker/opensky-consumer/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=180 --retries 10 -r requirements.txt

COPY ingestion/opensky_stream ./ingestion/opensky_stream

CMD ["python", "ingestion/opensky_stream/kafka_to_gcs.py"]

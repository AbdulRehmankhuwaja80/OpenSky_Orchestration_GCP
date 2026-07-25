# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY docker/dlt/requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --default-timeout=180 --retries 10 -r requirements.txt

COPY ingestion/openaq_batch ./ingestion/openaq_batch

CMD ["python", "ingestion/openaq_batch/openaq_pipeline.py"]

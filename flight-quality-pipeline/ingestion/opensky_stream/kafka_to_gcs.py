"""Consumes OpenSky Network aircraft state vectors from Kafka and writes them as JSON-lines
batches to the GCS data lake bronze layer (bronze/opensky/) - the ingestion-layer counterpart to
dlt's OpenAQ batch pipeline. Spark is kept purely for transformation (bronze -> silver -> BigQuery),
not ingestion.
"""
import json
import os
import time

from dotenv import load_dotenv
from google.cloud import storage
from kafka import KafkaConsumer

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC_OPENSKY"]
BUCKET_NAME = os.environ["GCP_BUCKET_NAME"]

BATCH_SIZE = 500  # flush after this many messages...
BATCH_INTERVAL_SECONDS = 30  # ...or after this long, whichever comes first

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="opensky-to-gcs",
    consumer_timeout_ms=5000,  # wake up periodically even with no new messages, so the
    # time-based flush below still fires during quiet periods instead of blocking forever.
)

bucket = storage.Client().bucket(BUCKET_NAME)


def flush(batch: list) -> None:
    if not batch:
        return
    body = "\n".join(json.dumps(record) for record in batch)
    blob_name = f"bronze/opensky/{int(time.time() * 1000)}.jsonl"
    bucket.blob(blob_name).upload_from_string(body, content_type="application/json")
    print(f"Wrote {len(batch)} messages to gs://{BUCKET_NAME}/{blob_name}")


def run() -> None:
    batch: list = []
    last_flush = time.time()
    while True:
        for message in consumer:
            batch.append(message.value)
            if len(batch) >= BATCH_SIZE:
                flush(batch)
                batch = []
                last_flush = time.time()
        if batch and (time.time() - last_flush) >= BATCH_INTERVAL_SECONDS:
            flush(batch)
            batch = []
            last_flush = time.time()


if __name__ == "__main__":
    run()

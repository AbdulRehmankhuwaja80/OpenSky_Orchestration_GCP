""" @bruin
name: bruin_openaq_silver
image: python:3.11
depends:
  - bruin_openaq_bronze
description: |
  Cleans the bronze OpenAQ readings (dedupe on sensor+timestamp, drop nulls, drop invalid
  values) and writes a single deduped parquet snapshot to the GCS silver layer. Plain script
  asset, same reasoning as openaq_bronze.py - Bruin's materialize() targets a warehouse
  connection, not a bucket path, so the bronze->silver step stays outside that mechanism.
@bruin """

import io
import json
import os
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(REPO_ROOT / "credentials" / "gcp-service-account.json")

BUCKET = os.environ["GCP_BUCKET_NAME"]
REQUIRED_COLUMNS = ["sensor_id", "value", "parameter", "datetime_from_utc"]


def read_bronze() -> pd.DataFrame:
    bucket = storage.Client().bucket(BUCKET)
    rows = []
    for blob in bucket.list_blobs(prefix="bronze/bruin_openaq/"):
        text = blob.download_as_text()
        rows.extend(json.loads(line) for line in text.splitlines() if line.strip())
    return pd.DataFrame(rows)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    if df.empty:
        print("No bronze rows to clean.")
        return df
    df = df.drop_duplicates(subset=["sensor_id", "datetime_from_utc"])
    df = df.dropna(subset=REQUIRED_COLUMNS)
    df = df[df["value"] >= 0]
    print(f"  {before} bronze rows -> {len(df)} after dedup/null/range cleaning")
    return df


def write_silver(df: pd.DataFrame) -> None:
    bucket = storage.Client().bucket(BUCKET)

    # Clear the target prefix before writing - relying on a library's own "overwrite" semantics
    # on GCS was already found (in the main pipeline, this session) to sometimes leave stale
    # files sitting alongside the new one, silently double-counted on the next read.
    for blob in bucket.list_blobs(prefix="silver/bruin_openaq/"):
        blob.delete()

    if df.empty:
        print("No rows survived cleaning - nothing written to silver.")
        return

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    blob_name = f"silver/bruin_openaq/{int(time.time() * 1000)}.parquet"
    bucket.blob(blob_name).upload_from_string(buffer.getvalue(), content_type="application/octet-stream")
    print(f"Wrote {len(df)} rows to gs://{BUCKET}/{blob_name}")


write_silver(clean(read_bronze()))

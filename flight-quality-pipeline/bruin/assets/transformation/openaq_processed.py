""" @bruin
name: cl_airpollution_data.bruin_openaq_processed
image: python:3.11
connection: gcp-default
depends:
  - bruin_openaq_silver

materialization:
  type: table
  strategy: create+replace

columns:
  - name: sensor_id
    type: integer
    checks:
      - name: not_null
  - name: parameter
    type: string
    checks:
      - name: not_null
  - name: value
    type: float
    checks:
      - name: not_null
      - name: non_negative
  - name: country
    type: string
    checks:
      - name: not_null

description: |
  Loads the cleaned GCS silver/bruin_openaq/ parquet into BigQuery as
  cl_airpollution_data.bruin_openaq_processed. This is the only asset in the Bruin pipeline
  that touches BigQuery - everything upstream (bronze, silver) is plain GCS file handling.
  Uses create+replace, not append/merge: each run fully replaces the table with the current
  deduped snapshot, since the main pipeline's Spark jobs hit a real duplicate-accumulation bug
  this session from repeatedly appending on every run instead.
@bruin """

import io
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from dotenv import load_dotenv
from google.cloud import storage

REPO_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(REPO_ROOT / ".env")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(REPO_ROOT / "credentials" / "gcp-service-account.json")

BUCKET = os.environ["GCP_BUCKET_NAME"]


def materialize():
    bucket = storage.Client().bucket(BUCKET)
    frames = []
    for blob in bucket.list_blobs(prefix="silver/bruin_openaq/"):
        if not blob.name.endswith(".parquet"):
            continue
        frames.append(pq.read_table(io.BytesIO(blob.download_as_bytes())).to_pandas())
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

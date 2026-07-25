"""Loads the silver layer (GCS) into BigQuery analytics tables."""
import os

from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

BUCKET = os.environ["GCP_BUCKET_NAME"]
PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ["BIGQUERY_DATASET"]
# Scope which source(s) to (re)load - avoids duplicating an already-loaded source's rows in
# BigQuery when only the other one has new silver data. Default: both.
SOURCES = os.environ.get("TRANSFORM_SOURCES", "openaq,opensky").split(",")

spark = SparkSession.builder.appName("silver_to_bigquery").getOrCreate()

if "opensky" in SOURCES:
    try:
        opensky_silver = spark.read.parquet(f"gs://{BUCKET}/silver/opensky/")
        (
            opensky_silver.write.format("bigquery")
            .option("table", f"{PROJECT_ID}.{DATASET}.opensky_processed")
            .option("temporaryGcsBucket", BUCKET)
            # overwrite, not append - silver is already the full current deduped snapshot (see
            # bronze_to_silver.py), so BigQuery should always be replaced with it wholesale, not
            # have it stacked on top of whatever was loaded on a previous run.
            .mode("overwrite")
            .save()
        )
        print(f"Wrote {opensky_silver.count()} rows to {DATASET}.opensky_processed")
    except AnalysisException as exc:
        print(f"Skipping opensky: silver/opensky/ not readable yet ({exc})")

if "openaq" in SOURCES:
    try:
        openaq_silver = spark.read.parquet(f"gs://{BUCKET}/silver/openaq/")
        (
            openaq_silver.write.format("bigquery")
            .option("table", f"{PROJECT_ID}.{DATASET}.openaq_processed")
            .option("temporaryGcsBucket", BUCKET)
            # overwrite, not append - see the opensky branch above for why.
            .mode("overwrite")
            .save()
        )
        print(f"Wrote {openaq_silver.count()} rows to {DATASET}.openaq_processed")
    except AnalysisException as exc:
        print(f"Skipping openaq: silver/openaq/ not readable yet ({exc})")

spark.stop()

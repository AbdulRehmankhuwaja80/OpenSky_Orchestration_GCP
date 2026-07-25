"""Cleans bronze OpenAQ/OpenSky data and writes deduplicated, typed output to the silver layer."""
import os

from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

BUCKET = os.environ["GCP_BUCKET_NAME"]
# Scope which source(s) to (re)process - avoids duplicating an already-processed source's
# output when only the other one has new bronze data. Default: both.
SOURCES = os.environ.get("TRANSFORM_SOURCES", "openaq,opensky").split(",")

spark = SparkSession.builder.appName("bronze_to_silver").getOrCreate()


def _clear_prefix(prefix: str) -> None:
    # Spark's mode("overwrite") on the GCS connector does not reliably clear pre-existing
    # objects under the target path before writing (observed leaving old part-files from prior
    # runs sitting alongside the new one, silently double-counted on the next read) - clearing
    # the prefix ourselves first makes each run's output the single source of truth.
    bucket = storage.Client().bucket(BUCKET)
    for blob in bucket.list_blobs(prefix=prefix):
        blob.delete()


def _clean(df, *, dedup_subset, required_columns, valid_condition=None):
    """Shared cleaning steps: drop duplicate readings, drop rows missing a required field, and
    (optionally) drop rows whose values are outside a physically valid range.
    """
    before = df.count()

    df = df.dropDuplicates(dedup_subset) if dedup_subset else df.dropDuplicates()
    after_dedup = df.count()

    df = df.dropna(subset=required_columns)
    after_dropna = df.count()

    if valid_condition is not None:
        df = df.filter(valid_condition)
    after_valid = df.count()

    print(
        f"  {before} bronze rows -> {after_dedup} after dedup -> {after_dropna} after dropping "
        f"nulls -> {after_valid} after range checks"
    )
    return df


# OpenSky state vectors (written to bronze/opensky by the opensky-to-gcs Kafka consumer,
# ingestion/opensky_stream/kafka_to_gcs.py - Spark only transforms here, it doesn't ingest)
if "opensky" in SOURCES:
    try:
        opensky_bronze = spark.read.json(f"gs://{BUCKET}/bronze/opensky/")
        opensky_silver = _clean(
            opensky_bronze,
            dedup_subset=["icao24", "observed_at"]
            if {"icao24", "observed_at"} <= set(opensky_bronze.columns)
            else None,
            required_columns=["icao24", "latitude", "longitude", "observed_at"],
            valid_condition=(F.col("latitude").between(-90, 90))
            & (F.col("longitude").between(-180, 180)),
        )
        # overwrite, not append: bronze is cumulative (grows every ingestion run) and this job
        # always reprocesses the *entire* bronze folder, so silver must always be replaced with
        # the current full deduped snapshot - appending would re-stack the same rows on every run.
        _clear_prefix("silver/opensky/")
        opensky_silver.write.mode("overwrite").parquet(f"gs://{BUCKET}/silver/opensky/")
        print(f"Wrote {opensky_silver.count()} rows to silver/opensky/")
    except AnalysisException as exc:
        print(f"Skipping opensky: bronze/opensky/ not readable yet ({exc})")

# OpenAQ measurements: dlt writes the actual table under bronze/openaq/openaq_measurements/,
# alongside its own _dlt_loads/_dlt_pipeline_state/_dlt_version bookkeeping folders at the
# bronze/openaq/ level, which aren't parquet - so read the table subfolder directly.
if "openaq" in SOURCES:
    try:
        openaq_bronze = spark.read.parquet(f"gs://{BUCKET}/bronze/openaq/openaq_measurements/")
        openaq_silver = _clean(
            openaq_bronze,
            # A full-row dropDuplicates() can't catch the same sensor reading pulled again by a
            # later dlt run - dlt stamps every row with a unique _dlt_load_id/_dlt_id per
            # ingestion, so two copies of the identical measurement never compare as equal rows.
            # (sensor_id, timestamp) is the real business key: one reading per sensor per window.
            dedup_subset=["sensor_id", "period__datetime_from__utc"],
            required_columns=["value", "parameter", "location_id", "period__datetime_from__utc"],
            valid_condition=F.col("value") >= 0,
        )
        # overwrite, not append - see the opensky branch above for why.
        _clear_prefix("silver/openaq/")
        openaq_silver.write.mode("overwrite").parquet(f"gs://{BUCKET}/silver/openaq/")
        print(f"Wrote {openaq_silver.count()} rows to silver/openaq/")
    except AnalysisException as exc:
        print(f"Skipping openaq: bronze/openaq/openaq_measurements/ not readable yet ({exc})")

spark.stop()

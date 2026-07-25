# Bruin pipeline (standalone, OpenAQ)

A second, fully independent OpenAQ pipeline built with [Bruin](https://bruin-data.github.io/bruin/)
instead of dlt + Spark + dbt. It exists purely to demonstrate the Bruin tool end-to-end
(ingestion, transformation, quality checks) — it does not read from, write to, or otherwise
depend on anything in the rest of this repo, and nothing in the rest of the repo depends on it.

```
OpenAQ API
    │
    ▼
bruin_openaq_bronze          (plain Python asset -> gs://<bucket>/bronze/bruin_openaq/*.jsonl)
    │
    ▼
bruin_openaq_silver           (plain Python asset -> gs://<bucket>/silver/bruin_openaq/*.parquet)
    │
    ▼
cl_airpollution_data.bruin_openaq_processed   (materialize() -> BigQuery table, + 5 column checks)
```

## Why bronze/silver are plain Python, not Bruin-native

Bruin's native mechanism for loading data is a Python asset's `materialize()` function, which
returns a DataFrame that Bruin loads into a **warehouse connection** (BigQuery, Postgres, etc.).
There's no built-in "write raw files to a bucket" primitive for a Python asset. So the bronze and
silver steps are **plain script assets** (no `connection`/`materialization` in their `@bruin`
header) that call `google-cloud-storage` directly — same approach the main pipeline's Kafka
consumer uses for `bronze/opensky/`. Only the final step (`openaq_processed.py`) uses Bruin's
actual `materialize()` mechanism, loading the clean silver parquet into BigQuery.

## Separation from the main pipeline

| | Main pipeline | This Bruin pipeline |
|---|---|---|
| GCS bronze | `bronze/openaq/` | `bronze/bruin_openaq/` |
| GCS silver | `silver/openaq/` | `silver/bruin_openaq/` |
| BigQuery table | `openaq_processed` | `bruin_openaq_processed` |
| Runs via | Docker (`docker/dlt`, `docker/spark`) | Bruin CLI on the host |

Both pipelines read the same root `.env` for `GCP_PROJECT_ID` / `GCP_BUCKET_NAME` /
`BIGQUERY_DATASET` / `OPENAQ_API_KEY` / `OPENAQ_COUNTRIES` (reused, never modified), and the same
`credentials/gcp-service-account.json`. Since Bruin runs on the host rather than in Docker, its
assets resolve the credentials file's real host path directly instead of using the
container-internal `/credentials/...` path `GOOGLE_APPLICATION_CREDENTIALS` is set to in `.env`.

## Setup

1. Install the Bruin CLI (already done once for this project, but for reference):
   ```
   curl -LsSf https://getbruin.com/install/cli | sh
   ```
2. `.bruin.yml` in this folder already declares the `gcp-default` connection, pointing at
   `../credentials/gcp-service-account.json`. Nothing to change unless your project ID differs
   from `.env`'s `GCP_PROJECT_ID`.

## Running

**Important**: Bruin's config auto-discovery does not reliably find `.bruin.yml` when it's nested
inside an existing git repository that isn't itself a dedicated Bruin workspace (this repo's own
`.git` is at the project root, one level up) — pass `--config-file` explicitly every time:

```
cd bruin
bruin validate --config-file .bruin.yml .
bruin run --config-file .bruin.yml .
```

The first run downloads and caches Python dependencies per-asset via `uv` (isolated per-asset
virtual environments) — expect the first `bruin run` to take several minutes; subsequent runs are
much faster.

## Data volume

Deliberately small, same "learning project" philosophy as the main pipeline: 2 locations per
country (from `OPENAQ_COUNTRIES` in `.env`), ~15 rows per sensor, 7-day backfill window.

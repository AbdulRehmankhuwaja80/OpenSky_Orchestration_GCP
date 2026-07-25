# OpenSky_Aviation_Track_pipeline

End-to-end pipeline joining historical OpenAQ air-quality data with live OpenSky Network flight-tracking data,
so the dashboard can relate air traffic activity to pollution (flight counts, altitude and speed
as a proxy for regional air-traffic intensity, alongside pollutant concentration). Data volumes
are deliberately kept small throughout (limited backfill window, capped row counts, a couple of
fixed bounding-box regions) since this is a learning/presentation project, not a production-scale
pipeline.

```
OpenAQ Historical API ──────────> dlt ──────────────────────┐
                                                              ├─> GCS bronze/{openaq,opensky}
OpenSky Network API (poll) -> Kafka Producer -> Kafka -> Kafka consumer ┘        │
                                                                              ▼
                                                       Spark (bronze_to_silver)
                                                                              │
                                                                              ▼
                                                       GCS silver/{openaq,opensky}
                                                                              │
                                                                              ▼
                                                       Spark (silver_to_bigquery)
                                                                              │
                                                                              ▼
                                            BigQuery (cl_airpollution_data)
                                                                              │
                                                                              ▼
                          dbt: stg_openaq, stg_opensky -> marts/batch, marts/streaming, marts/correlation
                                                                              │
                                                                              ▼
                                                       Streamlit dashboard (historical, live, combined KPIs)

                              Kestra orchestrates the OpenAQ batch chain above (ingest -> Spark -> dbt).
                              The OpenSky producer + consumer run continuously, independent of Kestra.
```

GCP resources this project targets (all sourced from `.env`):
- Project: `cl-datapipeline-learn`
- GCS bucket: `cl-air-datapipeline-sss` (location: `US`, multi-region)
- BigQuery dataset: `cl_airpollution_data`

Bucket layout (see [data_lake/README.md](data_lake/README.md) for details):
```
bronze/openaq/          bronze/opensky/
silver/openaq/          silver/opensky/
```

## Folder structure

```
.
├── .env                        # real credentials (git-ignored)
├── .env.example                # template, safe to commit
├── credentials/                 # GCP service-account.json goes here (git-ignored)
├── data_lake/                   # GCS bucket layer/prefix conventions (docs)
├── warehouse/bigquery/ddl/      # BigQuery dataset/table notes + manual DDL
│
├── ingestion/
│   ├── openaq_batch/            # dlt pipeline -> GCS bronze layer (batch)
│   └── opensky_stream/          # producer.py (API poll -> Kafka), kafka_to_gcs.py (Kafka -> bronze/opensky)
│
├── transformation/
│   └── spark_jobs/              # bronze_to_silver.py, silver_to_bigquery.py (transformation only)
│
├── dbt/
│   └── air_quality_dbt/         # dbt project (BigQuery adapter, driven by .env via env_var())
│       └── models/
│           ├── staging/         # stg_openaq, stg_opensky
│           └── marts/
│               ├── batch/         # OpenAQ-only marts (daily summary, most polluted days)
│               ├── streaming/     # OpenSky-only marts (latest state vector per aircraft)
│               └── correlation/   # cross-domain marts (flight activity vs air quality, by country+day)
│
├── dashboard/
│   └── app.py                   # Streamlit dashboard (historical, live, combined KPIs)
│
├── orchestration/
│   └── kestra/flows/            # Kestra flow YAML (OpenAQ batch chain)
│
└── docker/                      # one docker-compose (+ Dockerfile) PER TOOL
    ├── kafka/
    ├── opensky-producer/         # OpenSky Network API -> Kafka
    ├── opensky-consumer/         # Kafka -> bronze/opensky (GCS)
    ├── dlt/
    ├── spark/
    ├── dbt/
    ├── dashboard/                # Streamlit
    └── kestra/
```

Each tool under `docker/` is deliberately isolated (its own `Dockerfile` + `docker-compose.yml`)
rather than one monolithic compose file, per the project requirement. They share a single
external Docker network so containers can reach each other by service name (e.g. `kafka:9092`).

## Setup

1. Create the shared network once:
   ```
   docker network create pipeline_net
   ```
2. Add your GCP service-account key at `credentials/gcp-service-account.json`
   (see [credentials/README.md](credentials/README.md) for required IAM roles).
3. Copy `.env.example` to `.env` and fill in your real values (API keys, GCP project/bucket/dataset).

## Running each service

```
docker compose -f docker/kafka/docker-compose.yml up -d

# OpenSky streaming leg (continuous):
docker compose -f docker/opensky-producer/docker-compose.yml up -d --build   # OpenSky Network API -> Kafka
docker compose -f docker/opensky-consumer/docker-compose.yml up -d --build   # Kafka -> bronze/opensky

# OpenAQ batch leg:
docker compose -f docker/dlt/docker-compose.yml up --build                       # -> bronze/openaq

# Shared transformation (both sources feed the same Spark jobs):
docker compose -f docker/spark/docker-compose.yml up -d --build spark-batch      # bronze -> silver

# silver -> BigQuery uses the same spark-batch image with an overridden command:
docker compose -f docker/spark/docker-compose.yml run -d --name spark-silver-to-bq spark-batch spark-submit \
  --master local[*] \
  --packages com.google.cloud.spark:spark-3.5-bigquery:0.41.0 \
  --jars /app/jars/gcs-connector-hadoop3-2.2.19.jar \
  --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
  --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
  /app/transformation/spark_jobs/silver_to_bigquery.py

docker compose -f docker/dbt/docker-compose.yml up --build
docker compose -f docker/kestra/docker-compose.yml up -d
docker compose -f docker/dashboard/docker-compose.yml up -d --build             # http://localhost:8501
```

**On Windows with Git Bash**: any command where you pass a `/app/...`-style container path directly
as a command-line argument (like the `run ... spark-submit ... --jars /app/jars/...` override above)
needs `MSYS_NO_PATHCONV=1` prefixed, e.g. `MSYS_NO_PATHCONV=1 docker compose -f ... run ...` —
otherwise Git Bash silently rewrites `/app/...` into a Windows path (`C:/Program Files/Git/app/...`)
before Docker ever sees it. This only affects paths typed directly at the Git Bash prompt; the
`command:` blocks already baked into the `docker-compose.yml` files are read from the YAML file
itself and are not affected.

`--jars` (a local file downloaded into the image) is used instead of `--packages` for the GCS
connector specifically because the Maven-resolved (`--packages`) copy pulls in a Guava version
that conflicts with the one already on Spark's classpath (`NoSuchMethodError` on
`Preconditions.checkState`) — see the `RUN curl ...` step in
[docker/spark/Dockerfile](docker/spark/Dockerfile) for where that jar comes from.

Kestra (http://localhost:8080) loads the flow definition from
`orchestration/kestra/flows/` and coordinates the OpenAQ batch chain (ingest -> Spark -> dbt) on a
schedule, so once it's running you don't need to invoke those commands manually. The OpenSky
producer/consumer are always-on services started independently via `docker compose up -d` (above)
— Kestra doesn't manage them, since they're continuous, not scheduled batch jobs.

## dbt

`dbt/air_quality_dbt/profiles.yml` reads `GCP_PROJECT_ID`, `BIGQUERY_DATASET`, `GCP_LOCATION`, and
`GOOGLE_APPLICATION_CREDENTIALS` straight from `.env` via `env_var()`, so there's a single source
of truth for GCP resource names.

| Model | Path | Notes |
|---|---|---|
| `stg_openaq` | `models/staging/stg_openaq.sql` | from `openaq_processed` |
| `stg_opensky` | `models/staging/stg_opensky.sql` | from `opensky_processed` |
| `mart_openaq_daily_summary` | `models/marts/batch/` | avg/min/max per country/location/parameter/day |
| `mart_openaq_most_polluted_days` | `models/marts/batch/` | top 20 days by avg PM2.5 |
| `mart_opensky_latest` | `models/marts/streaming/` | latest state vector per aircraft |
| `mart_flight_air_quality_correlation` | `models/marts/correlation/` | daily OpenAQ + OpenSky aggregates joined on country+day |

## Dashboard

`dashboard/app.py` (Streamlit) reads the marts above directly from BigQuery and renders three
tabs: **Historical (OpenAQ)** — avg PM2.5/PM10/NO2/O3, pollution trend, most polluted days;
**Live (OpenSky)** — current altitude/ground speed/heading/vertical rate per tracked aircraft; **Combined** —
flight count vs PM2.5, avg altitude vs PM2.5, avg ground speed vs PM10, flight count vs NO2.

## Notes / next steps

- The Kestra flow and Spark BigQuery/GCS connector versions are starting points — adjust plugin
  versions to whatever Kestra/Spark release you install.
- `OPENAQ_MAX_ROWS_PER_SENSOR`, `OPENAQ_MAX_LOCATIONS_PER_COUNTRY`, `OPENAQ_BACKFILL_DAYS`, and the
  small `OPENSKY_BBOXES` list in `.env` keep data volume small on purpose while still
  spanning every pollutant/location/country the dashboard needs — raise them once you outgrow the
  learning/demo scope.

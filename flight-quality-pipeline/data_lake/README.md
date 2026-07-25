# Data Lake Layout — gs://cl-air-datapipeline-sss (location: US, multi-region)

| Prefix | Populated by | Contents |
|---|---|---|
| `bronze/openaq/` | dlt (`ingestion/openaq_batch`) | Historical OpenAQ air quality measurements, as returned by the API |
| `bronze/opensky/` | Kafka consumer (`ingestion/opensky_stream/kafka_to_gcs.py`) | Untouched OpenSky Network aircraft state vectors, read off the Kafka topic |
| `silver/openaq/` | Spark batch (`bronze_to_silver.py`) | Deduplicated OpenAQ measurements |
| `silver/opensky/` | Spark batch (`bronze_to_silver.py`) | Deduplicated OpenSky state vectors |

Ingestion tools (dlt, the Kafka consumer) land bronze data directly in the bucket; Spark is used
purely for transformation (`bronze/` -> `silver/` -> BigQuery), never for ingestion.

`silver/openaq/` and `silver/opensky/` are then loaded into BigQuery
(dataset `cl_airpollution_data`) by `silver_to_bigquery.py` as tables
`openaq_processed` and `opensky_processed`.

This bucket and prefix convention is referenced by the Spark jobs in
[transformation/spark_jobs](../transformation/spark_jobs), the dlt pipeline in
[ingestion/openaq_batch](../ingestion/openaq_batch), and the Kafka consumer in
[ingestion/opensky_stream](../ingestion/opensky_stream).

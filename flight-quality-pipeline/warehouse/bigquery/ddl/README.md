# BigQuery Warehouse — project `cl-datapipeline-learn`, dataset `cl_airpollution_data`

Tables in this dataset are created by the Spark job
([transformation/spark_jobs/silver_to_bigquery.py](../../../transformation/spark_jobs/silver_to_bigquery.py)),
which reads from the bucket's `silver/` layer:

- `openaq_processed` — cleaned/deduplicated OpenAQ historical air quality data
- `opensky_processed` — cleaned/deduplicated OpenSky Network aircraft state vectors

These two tables are declared as dbt sources in
[dbt/air_quality_dbt/models/staging/sources.yml](../../../dbt/air_quality_dbt/models/staging/sources.yml)
and consumed by the staging/marts models.

Add any manual DDL (partitioning, clustering, IAM-scoped views, etc.) as `.sql` files in this folder.

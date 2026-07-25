# Credentials

Place your GCP service account key here as `gcp-service-account.json`.

Required IAM roles for `gcp-service-account@cl-datapipeline-learn.iam.gserviceaccount.com`:
- **Storage Admin** (`roles/storage.admin`) on bucket `cl-air-datapipeline-sss` — Storage Object Admin is
  NOT enough on its own: it covers object get/list/delete but not `storage.buckets.get`, which dlt/gcsfs
  and Spark's GCS connector both check when opening the bucket, and its absence surfaces as a
  confusing "bucket does not exist" / 403 error instead of a clear permissions message.
- BigQuery Data Editor
- BigQuery Job User

This folder is mounted read-only into the `dlt`, `opensky-consumer`, `spark`, `dbt`, and
`dashboard` containers (see their respective `docker-compose.yml` files) and is git-ignored —
never commit real keys.

-- ============================================================
-- Data validation checks for cl_airpollution_data.openaq_processed
-- Project: cl-datapipeline-learn, Dataset: cl_airpollution_data
-- Run these in the BigQuery Console (Query editor) or `bq query --use_legacy_sql=false`
-- ============================================================

-- 0. List actual columns (safest first step - confirms schema before the queries below)
SELECT column_name, data_type
FROM `cl-datapipeline-learn.cl_airpollution_data.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'openaq_processed'
ORDER BY ordinal_position;

-- 1. Row count
SELECT COUNT(*) AS row_count
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`;

-- 2. Sample rows
SELECT *
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`
LIMIT 10;

-- 3. Breakdown by country/parameter, with reading time range
SELECT
  country,
  parameter,
  COUNT(*) AS measurement_count,
  MIN(period__datetime_from__utc) AS earliest_reading,
  MAX(period__datetime_from__utc) AS latest_reading
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`
GROUP BY country, parameter
ORDER BY country, parameter;

-- 4. Null / data-quality spot check on key columns
SELECT
  COUNTIF(value IS NULL) AS null_value_count,
  COUNTIF(sensor_id IS NULL) AS null_sensor_id_count,
  COUNTIF(location_id IS NULL) AS null_location_id_count
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`;

-- 5. Duplicate check (same sensor reporting the same reading window twice)
SELECT
  sensor_id,
  period__datetime_from__utc,
  COUNT(*) AS dup_count
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`
GROUP BY sensor_id, period__datetime_from__utc
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 20;

-- 6. Distinct locations/sensors actually loaded (sanity check on backfill scope)
SELECT
  COUNT(DISTINCT location_id) AS distinct_locations,
  COUNT(DISTINCT sensor_id) AS distinct_sensors
FROM `cl-datapipeline-learn.cl_airpollution_data.openaq_processed`;

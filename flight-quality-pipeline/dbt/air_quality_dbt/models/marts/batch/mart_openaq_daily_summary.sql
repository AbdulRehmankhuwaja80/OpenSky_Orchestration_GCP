-- Daily air-quality summary per location/parameter, built entirely from the batch (OpenAQ)
-- side of the pipeline - no dependency on the (not yet implemented) Coinbase streaming data.
-- Grain: one row per country + location + parameter + day.
select
    country,
    location_id,
    location_name,
    parameter,
    date(period__datetime_from__utc) as reading_date,
    count(*) as reading_count,
    avg(value) as avg_value,
    min(value) as min_value,
    max(value) as max_value
from {{ ref('stg_openaq') }}
group by country, location_id, location_name, parameter, reading_date

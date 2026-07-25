-- Top 20 most polluted days (by average PM2.5) across all locations - feeds the "most polluted
-- days" KPI on the historical (OpenAQ) side of the dashboard.
select
    country,
    location_id,
    location_name,
    reading_date,
    avg_value as avg_pm25,
    reading_count
from {{ ref('mart_openaq_daily_summary') }}
where parameter = 'pm25'
order by avg_pm25 desc
limit 20

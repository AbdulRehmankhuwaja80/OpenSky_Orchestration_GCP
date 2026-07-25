-- Combined KPIs: joins daily OpenAQ pollutant averages with daily OpenSky flight-activity
-- aggregates, both at country + day grain (OpenAQ sensor locations and OpenSky bounding-box
-- regions don't share exact coordinates, so country+day is the common denominator; origin_country
-- from the state vector is used as the join key on the flight side). Feeds the dashboard's
-- "flight count vs PM2.5", "avg altitude vs PM2.5" and "avg speed vs PM10" charts.
with openaq_daily as (
    select
        country,
        reading_date,
        avg(case when parameter = 'pm25' then avg_value end) as avg_pm25,
        avg(case when parameter = 'pm10' then avg_value end) as avg_pm10,
        avg(case when parameter = 'no2' then avg_value end) as avg_no2,
        avg(case when parameter = 'o3' then avg_value end) as avg_o3
    from {{ ref('mart_openaq_daily_summary') }}
    group by country, reading_date
),

opensky_daily as (
    select
        origin_country as country,
        date(observed_at) as reading_date,
        count(distinct icao24) as flight_count,
        avg(baro_altitude) as avg_altitude,
        avg(velocity) as avg_velocity
    from {{ ref('stg_opensky') }}
    group by origin_country, reading_date
)

select
    coalesce(a.country, f.country) as country,
    coalesce(a.reading_date, f.reading_date) as reading_date,
    a.avg_pm25,
    a.avg_pm10,
    a.avg_no2,
    a.avg_o3,
    f.flight_count,
    f.avg_altitude,
    f.avg_velocity
from openaq_daily a
full outer join opensky_daily f
    on a.country = f.country
    and a.reading_date = f.reading_date

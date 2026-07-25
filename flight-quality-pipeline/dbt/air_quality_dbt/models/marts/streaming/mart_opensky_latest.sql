-- Latest state vector per aircraft - feeds the "Live" (OpenSky) KPI cards on the dashboard
-- (position, altitude, ground speed, heading, on-ground status).
with ranked as (
    select
        *,
        row_number() over (partition by icao24 order by observed_at desc) as rn
    from {{ ref('stg_opensky') }}
)

select
    region,
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    baro_altitude,
    geo_altitude,
    on_ground,
    velocity,
    true_track,
    vertical_rate,
    observed_at
from ranked
where rn = 1

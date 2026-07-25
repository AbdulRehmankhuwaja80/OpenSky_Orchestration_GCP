select *
from {{ source('air_quality', 'opensky_processed') }}

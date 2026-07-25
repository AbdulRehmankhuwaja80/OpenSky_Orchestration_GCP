select *
from {{ source('air_quality', 'openaq_processed') }}

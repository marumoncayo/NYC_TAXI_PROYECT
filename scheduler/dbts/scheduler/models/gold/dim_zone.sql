{{ config(
    materialized='table'
    
) }}

select
    row_number() over (order by LOCATIONID) as zone_sk,
    LOCATIONID as zone_id,
    BOROUGH as borough,
    ZONE as zone_name,
    SERVICE_ZONE as service_zone
from {{ source('raw', 'taxi_zones') }}

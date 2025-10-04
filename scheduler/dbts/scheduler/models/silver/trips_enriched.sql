{{ config(
    materialized='table'
) }}


with combined as (
    select 
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        payment_type,
        pulocationid,
        dolocationid,
        'yellow' as service_type
    from {{ ref('yellow_trips') }}

    union all

    select 
        pickup_datetime,
        dropoff_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        payment_type,
        pulocationid,
        dolocationid,
        'green' as service_type
    from {{ ref('green_trips') }}
),


cleaned as (
    select *
    from combined
    where pickup_datetime is not null
      and dropoff_datetime is not null
      and trip_distance > 0
      and fare_amount >= 0
      and passenger_count between 0 and 9
)

select *
from cleaned

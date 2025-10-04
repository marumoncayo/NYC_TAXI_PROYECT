


{{ config(materialized='view') }}

with raw_data as (
    select
        TPEP_PICKUP_DATETIME,
        TPEP_DROPOFF_DATETIME,
        PASSENGER_COUNT,
        TRIP_DISTANCE,
        FARE_AMOUNT,
        TIP_AMOUNT,
        PAYMENT_TYPE,
        PULOCATIONID,
        DOLOCATIONID
    from {{ source('raw', 'yellow_trips') }}
)

select
    to_timestamp_ntz(TPEP_PICKUP_DATETIME / 1000000000) as pickup_datetime,
    to_timestamp_ntz(TPEP_DROPOFF_DATETIME / 1000000000) as dropoff_datetime,
    PASSENGER_COUNT as passenger_count,
    TRIP_DISTANCE as trip_distance,
    FARE_AMOUNT as fare_amount,
    TIP_AMOUNT as tip_amount,
    PAYMENT_TYPE as payment_type,
    PULOCATIONID as pulocationid,
    DOLOCATIONID as dolocationid,
    'yellow' as service_type,
    year(to_timestamp_ntz(TPEP_PICKUP_DATETIME / 1000000000)) as pickup_year,
    month(to_timestamp_ntz(TPEP_PICKUP_DATETIME / 1000000000)) as pickup_month
from raw_data
where to_timestamp_ntz(TPEP_PICKUP_DATETIME / 1000000000) < '2025-09-01'::timestamp_ntz
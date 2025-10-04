{{ config(
    materialized='table'
) }}

with date_spine as (
    select distinct pickup_datetime::date as date_day
    from {{ ref('stg_all_trips') }}
    union
    select distinct dropoff_datetime::date as date_day
    from {{ ref('stg_all_trips') }}
)

select
    row_number() over (order by date_day) as date_sk,
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week,
    dayname(date_day) as day_name,
    quarter(date_day) as quarter,
    weekofyear(date_day) as week_of_year
from date_spine

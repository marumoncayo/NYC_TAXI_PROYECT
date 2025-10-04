{{ config(
    materialized='table',
   
    cluster_by=['pickup_date_sk', 'service_type']
) }}

with trips as (
    select * from {{ ref('trips_enriched') }}
),

dim_date as (
    select * from {{ ref('dim_date') }}
),

dim_zone as (
    select * from {{ ref('dim_zone') }}
),

dim_payment_type as (
    select * from {{ ref('dim_payment_type') }}
)

select
    row_number() over (order by t.pickup_datetime) as trip_sk,

  
    dd_pu.date_sk as pickup_date_sk,
    dd_do.date_sk as dropoff_date_sk,
    dz_pu.zone_sk as pickup_zone_sk,
    dz_do.zone_sk as dropoff_zone_sk,
    dpt.payment_type_sk,
    
   
    t.service_type,
    t.pickup_datetime,
    t.dropoff_datetime,
    date_part(hour, t.pickup_datetime) as pickup_hour,
    
   
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.fare_amount + coalesce(t.tip_amount, 0) as total_amount,
    
    
    datediff('minute', t.pickup_datetime, t.dropoff_datetime) as trip_duration_minutes,
    case 
        when datediff('minute', t.pickup_datetime, t.dropoff_datetime) > 0
        then (t.trip_distance / (datediff('minute', t.pickup_datetime, t.dropoff_datetime) / 60.0))
        else 0 
    end as avg_speed_mph

from trips t
left join dim_date dd_pu on t.pickup_datetime::date = dd_pu.date_day
left join dim_date dd_do on t.dropoff_datetime::date = dd_do.date_day
left join dim_zone dz_pu on t.pulocationid = dz_pu.zone_id
left join dim_zone dz_do on t.dolocationid = dz_do.zone_id
left join dim_payment_type dpt on t.payment_type = dpt.payment_type_code
where t.pickup_datetime is not null
  and t.dropoff_datetime is not null

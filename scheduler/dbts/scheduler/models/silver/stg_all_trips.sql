{{ config(materialized='view') }}

select * from {{ ref('green_trips') }}
union all
select * from {{ ref('yellow_trips') }}

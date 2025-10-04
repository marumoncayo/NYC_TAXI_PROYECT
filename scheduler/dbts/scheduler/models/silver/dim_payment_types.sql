{{ config(
    materialized='table'
) }}

with payment_types as (
    select distinct payment_type
    from {{ ref('stg_all_trips') }}
    where payment_type is not null
)

select
    row_number() over (order by payment_type) as payment_type_sk,
    payment_type as payment_type_code,
    case payment_type
        when 1 then 'Credit card'
        when 2 then 'Cash'
        when 3 then 'No charge'
        when 4 then 'Dispute'
        when 5 then 'Unknown'
        when 6 then 'Voided trip'
        else 'Other'
    end as payment_type_name
from payment_types

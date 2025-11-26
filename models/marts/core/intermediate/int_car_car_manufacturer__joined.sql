{{ 
    config(
        materialized='ephemeral', 
    )
}}

with cm as (
    select * from {{ ref("stg_nascar__car_manufacturer") }}
),
joined as (
    select 
        ca.car_id as car_id,
        cm.car_manufacturer as car_manufacturer,
        ca.car_number as car_number,
        ca.synced_at as synced_at
    from {{ ref("stg_nascar__car") }} ca
    join cm on cm.car_manufacturer_id = ca.car_manufacturer_id

)

select * from joined
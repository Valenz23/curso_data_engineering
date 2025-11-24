{{ 
    config(
        materialized='incremental', 
        unique_key='car_id'
    )
}}

with stg as (
    select * from {{ ref("int_car_car_manufacturer__joined") }}
),
dim as (
    select
        car_id,
        car_manufacturer,
        car_number,
        synced_at
    from stg
)
select * from dim

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
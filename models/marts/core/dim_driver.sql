{{ 
    config(
        materialized='incremental', 
        unique_key='driver_id'
    )
}}


with stg as (
    select * from {{ ref("stg_nascar__driver") }}
),
dim as (
    select       
        driver_id,
        driver_name,
        synced_at
    from stg
)

select * from dim

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
{{ 
    config(
        materialized='incremental', 
        unique_key='driver_id'
    )
}}

with base as (
    select * from {{ref("base_nascar__driver")}}
),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['driver']) }} as driver_id,       
        driver::varchar(50) as driver_name,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
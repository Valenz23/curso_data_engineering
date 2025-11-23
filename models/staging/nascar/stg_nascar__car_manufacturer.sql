{{ 
    config(
        materialized='incremental', 
        unique_key='car_manufacturer_id'
    )
}}

with base as (
    select * from {{ref("base_nascar__car_manufacturer")}}
),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['manu']) }} as car_manufacturer_id,
        manu::varchar(20) as car_manufacturer,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
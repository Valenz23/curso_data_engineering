{{ 
    config(
        materialized='incremental', 
        unique_key='car_id'
    )
}}

with base as (
    select * from {{ref("base_nascar__car")}}
),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['car_num', 'manu']) }} as car_id,
        manu::varchar(20) as car_manufacturer,
        car_num::number(2,0) as car_number,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
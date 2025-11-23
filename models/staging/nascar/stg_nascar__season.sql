{{ 
    config(
        materialized='incremental', 
        unique_key='season_id'
    )
}}

with base as ( 
    select * from {{ ref("base_nascar__season") }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year']) }} as season_id,
        year::number(4,0) as season_year,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
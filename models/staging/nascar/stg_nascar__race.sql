{{ 
    config(
        materialized='incremental', 
        unique_key='race_id'
    ) 
}}

with base as (
    select * from {{ref("base_nascar__race")}}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['year','race_num']) }} as race_id,
        {{ dbt_utils.generate_surrogate_key(['year']) }} as season_id,
        {{ dbt_utils.generate_surrogate_key(['track']) }} as track_id,
        race_num::number(2) as race_number,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
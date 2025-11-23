{{ 
    config(
        materialized='incremental', 
        unique_key='track_type_id'
    )
}}

with base as ( 
    select * from {{ ref("base_nascar__track_type") }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['track_type']) }} as track_type_id,
        track_type::varchar(20) as track_type_desc,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
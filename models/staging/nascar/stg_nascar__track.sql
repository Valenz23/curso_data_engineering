{{ 
    config(
        materialized='incremental', 
        unique_key='track_id'
    )
}}

with base as ( 
    select * from {{ ref("base_nascar__track") }}
),
renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['track']) }} as track_id,
        {{ dbt_utils.generate_surrogate_key(['track_type']) }} as track_type_id,
        -- Truncar a 20 caracteres antes del cast
        case 
            when length(track) > 20 then left(track, 20)
            else track
        end::varchar(20) as track_name,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
with base as ( 
    select * from {{ ref("base_nascar__nascar_results") }}
),
selected as (
    select
        distinct {{ dbt_utils.generate_surrogate_key(['track']) }} as track_id,
        -- {{ dbt_utils.generate_surrogate_key(['track_type']) }} as track_type_id,
        track as track_name
    from base
)

select * from selected
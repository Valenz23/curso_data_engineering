with source as (
    select * from {{ ref('base_nascar__nascar_results') }}
),
distinct_track_types as (
    select distinct
        track_type::varchar(50) as track_type_name
    from source
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['track_type_name']) }} as track_type_id,
        track_type_name
    from distinct_track_types
)

select * from final

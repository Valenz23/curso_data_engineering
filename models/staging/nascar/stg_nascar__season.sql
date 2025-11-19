with source as (
    select * from {{ ref('base_nascar__nascar_results') }}
),
distinct_season as (
    select distinct
        year::number(4,0) as season_year
    from source
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['season_year']) }} as season_id,
        season_year
    from distinct_season
)

select * from final

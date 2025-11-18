with source as (
    select * from {{ source('nascar', 'nascar_results') }}
),
distinct_team_names as (
    select distinct
        team_name::varchar(50) as team_name
    from source
),
final as (
    select
        {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id,
        team_name
    from distinct_team_names
)

select * from final

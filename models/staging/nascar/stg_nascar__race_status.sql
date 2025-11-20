with base as (
    select * from {{ ref('base_nascar__nascar_results') }}
),
distinct_race_status as (
    select 
        distinct lower(status)::varchar(50) as race_status_desc
    from base
),
case_race_status as (
    select
        case 
            when race_status_desc = 'running' then 'Completed'
            when race_status_desc = 'running/dq' then 'DQ'
            else 'DNF'
        end as race_status_desc
    from distinct_race_status
),
final as (
    select distinct 
        {{ dbt_utils.generate_surrogate_key(['race_status_desc']) }} as race_status_id,
        race_status_desc
    from case_race_status
)

select * from final

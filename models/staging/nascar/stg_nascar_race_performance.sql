with base as (
    select * from {{ ref('base_nascar__nascar_results') }}
),
selected as (
    select 
        {{ dbt_utils.generate_surrogate_key(['year','track','track_type','race_num']) }} as race_id,
        {{ dbt_utils.generate_surrogate_key(['driver','car_num','manu','team_name']) }} as driver_assigment_id,
        case 
            when lower(status) = 'running' then {{ dbt_utils.generate_surrogate_key(["'Completed'"]) }}
            when lower(status) = 'running/dq' then {{ dbt_utils.generate_surrogate_key(["'DQ'"]) }}
            else {{ dbt_utils.generate_surrogate_key(["'DNF'"]) }}
        end as race_status_id,
        "start"::number(2) as start_pos,
        fin::number(2) as final_pos,
        stage_1::number(2) as stage_1_pos,
        stage_2::number(2) as stage_2_pos,
        stage_3_or_duel::number(2) as stage_3_or_duel_pos,
        laps::number(3) as laps_completed,
        laps_led::number(3) as laps_led,
        stage_points::number(2) as stage_points,
        points::number(2) as total_points
    from base
)

select * from selected
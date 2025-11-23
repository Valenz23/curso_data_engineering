{{ 
    config(
        materialized='incremental', 
        unique_key='race_perf_id'
    ) 
}}

with base as (
    select * from {{ ref('base_nascar__race_performance') }}
),
renamed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['year','race_num']) }} as race_id,
        {{ dbt_utils.generate_surrogate_key(['driver']) }} as driver_id,       
        {{ dbt_utils.generate_surrogate_key(['team_name']) }} as team_id,
        {{ dbt_utils.generate_surrogate_key(['car_num', 'manu']) }} as car_id,
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
        CASE WHEN race_num > 0 and fin = 1 THEN 5 ELSE 0 END
            + CASE WHEN race_num > 0 and stage_1 = 1 THEN 1 ELSE 0 END
            + CASE WHEN race_num > 0 and stage_2 = 1 THEN 1 ELSE 0 END
            + CASE WHEN race_num > 0 and stage_3_or_duel = 1 THEN 1 ELSE 0 END
        as playoff_points,
        ifnull(points::number(2),0) as points,
        ifnull((playoff_points+points)::number(2),0) as total_points,
        synced_at
    from base
)

select * from renamed

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
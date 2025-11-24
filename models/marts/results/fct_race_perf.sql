{{ 
    config(
        materialized='incremental', 
        unique_key=['race_id', 'driver_id', 'car_id', 'team_id', 'race_status_id']
    )
}}


with 
ra as (select * from {{ ref("dim_race") }}),
dr as (select * from {{ref("dim_driver")}}),
ca as (select * from {{ref("dim_car")}}),
te as (select * from {{ref("dim_team")}}),
st as (select * from {{ref("dim_race_status")}}),
joined as (
    select
        pe.race_id as race_id,
        pe.driver_id as driver_id,
        pe.car_id as car_id,
        pe.team_id as team_id,
        pe.race_status_id as race_status_id,
        pe.start_pos as start_pos,
        pe.final_pos as final_pos,
        pe.stage_1_pos as stage_1_pos,
        pe.stage_2_pos as stage_2_pos,
        pe.stage_3_or_duel_pos as stage_3_or_duel_pos,
        pe.laps_completed as laps_completed,
        pe.laps_led as laps_led,
        pe.stage_points as stage_points,
        pe.playoff_points as playoff_points,
        pe.points as points,
        pe.total_points as total_points,
        pe.synced_at as synced_at
    from {{ref("stg_nascar__race_perf")}} pe

    join ra on pe.race_id=ra.race_id
    join dr on pe.driver_id=dr.driver_id
    join ca on pe.car_id=ca.car_id
    join te on pe.team_id=te.team_id
    join st on pe.race_status_id=st.race_status_id

)

select * from joined

{% if is_incremental() %}
  where synced_at > (select max(synced_at) from {{ this }})
{% endif %}
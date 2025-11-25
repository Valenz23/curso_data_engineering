with dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
base as (
    select
        rp.race_id,
        ra.season,
        ra.track_name,
        dr.driver_name,
        case when rp.final_pos=1 then 1 else 0 end as win,
        rp.laps_led,
        max(rp.laps_completed) over (partition by rp.race_id) as max_laps
    from {{ ref("stg_nascar__race_perf") }} rp
    join dr on dr.driver_id=rp.driver_id
    join ra on ra.race_id=rp.race_id
),
cte as (
    select
        season,
        track_name,
        driver_name,
        laps_led,
        max_laps,
        round((laps_led / nullif(max_laps,0)) * 100,2) as pct_win_laps_led
    from base
    having win = 1
)

select * from cte
{{ 
    config(
        materialized='ephemeral', 
    )
}}

with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
rs as (
    select 
        ra.season,
        dr.driver_name as driver,
        count(case when rp.final_pos=1 then 1 end) as victories,
        -- sum(rp.playoff_points) as playoff_points,
        sum(rp.total_points) as total_points
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id=rp.driver_id
    join ra on ra.race_id=rp.race_id
    where ra.race_number between 1 and 26
    group by driver, season
    order by victories desc, total_points desc
)

select * from rs
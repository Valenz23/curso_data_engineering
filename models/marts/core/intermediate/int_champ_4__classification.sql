with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
r8 as (select * from {{ref("int_round_8__classification")}}),

c4 as (
    select top 4
        r8.season as season,
        r8.driver as driver,
        count(case when rp.final_pos=1 then 1 end) as victories,
        sum(rp.total_points) as total_points
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id = rp.driver_id
    join ra on ra.race_id=rp.race_id
    join r8 on r8.driver = dr.driver_name
    where ra.race_number between 33 and 35
    group by driver, r8.season
    order by victories desc, total_points desc

)

select * from c4
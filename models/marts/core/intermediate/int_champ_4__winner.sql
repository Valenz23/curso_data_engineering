{{ 
    config(
        materialized='ephemeral', 
    )
}}

with 
dr as (select * from {{ref("dim_driver")}}),
ra as (select * from {{ref("dim_race")}}),
c4 as (select * from {{ref("int_champ_4__classification")}}),

winner as (
    select 
        c4.season as season,
        c4.driver as driver,
        rp.final_pos as final_pos
    from {{ref("stg_nascar__race_perf")}} rp
    join dr on dr.driver_id = rp.driver_id
    join ra on ra.race_id=rp.race_id
    join c4 on c4.driver = dr.driver_name
    where ra.race_number = 36
    order by final_pos

)

select * from winner
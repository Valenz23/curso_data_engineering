with 
rs as (select * from {{ref("int_champ_4__winner")}}),

winner as (
    select
        season,
        driver,
        final_pos
    from rs
    order by season
)

select * from winner
with 
rs as (select * from {{ref("int_champ_4__classification")}}),

champ4 as (
    select
        season,
        driver,
        victories,
        total_points
    from rs
)

select * from champ4
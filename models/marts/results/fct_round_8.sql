with 
rs as (select * from {{ref("int_round_8__classification")}}),

top8 as (
    select
        season,
        driver,
        victories,
        total_points
    from rs
)

select * from top8
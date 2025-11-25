with 
rs as (select * from {{ref("int_regular_season__classification")}}),

top16 as (
    select top 16
        season,
        driver,
        victories,
        total_points
    from rs
)

select * from top16
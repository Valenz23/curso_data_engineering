with src as (
    select * from {{source("nascar","nascar_results")}}
),
renamed as (
    select
        year,       
        race_num,
        track,
        track_type,
        fin,
        "start",
        car_num,
        driver,
        manu,
        team_name,
        laps, 
        laps_led,
        status,
        points,
        stage_1,
        stage_2,
        stage_3_or_duel,
        stage_points
    from src
)

select * from renamed
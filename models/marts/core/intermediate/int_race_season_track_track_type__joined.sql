{{ 
    config(
        materialized='ephemeral', 
    )
}}

with tr as (select * from {{ref("int_track_track_type__joined")}}),
se as (select * from {{ref("stg_nascar__season")}}),
joined as (
    select
        ra.race_id as race_id,
        se.year as season,
        tr.track_name as track_name,
        tr.track_type_desc as track_type,
        ra.race_number as race_number,
        ra.synced_at as synced_at
    from {{ref("stg_nascar__race")}} ra
    join tr on tr.track_id=ra.track_id
    join se on se.season_id=ra.season_id
)

select * from joined
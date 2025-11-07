{{
    config(
        materialized='view'
    )
}}

with src_events as (
    select *
    from {{ source("SQL_SERVER_DBO", "EVENTS") }}
),
renamed_casted AS (
    SELECT

        -- aaaaaaaaaaaaaaaaaa
        
    FROM src_events
    )

SELECT * FROM renamed_casted
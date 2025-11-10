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

        event_id,
        page_url,
        event_type, -- sacar a otra tabla
        user_id,
        product_id,
        session_id,
        convert_timezone('UTC',created_at) as created_at,
        order_id,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
        
    FROM src_events
    )

SELECT * FROM renamed_casted
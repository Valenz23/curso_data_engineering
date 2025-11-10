{{
    config(
        materialized='view'
    )
}}

with src_users as (
    select *
    from {{ source("SQL_SERVER_DBO", "USERS") }}
),
renamed_casted AS (
    SELECT
        user_id,
        first_name,
        last_name,
        phone_number,
        email,
        total_orders,
        updated_at,
        convert_timezone('UTC',created_at) as created_at,
        address_id,
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,        
        _fivetran_deleted AS date_deleted
    FROM src_users
    )

SELECT * FROM renamed_casted
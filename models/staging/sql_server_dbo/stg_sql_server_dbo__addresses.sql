{{
    config(
        materialized='view'
    )
}}

with src_addresses as (
    select *
    from {{ source("SQL_SERVER_DBO", "ADDRESSES") }}
),
renamed_casted AS (
    SELECT
        address_id, 
        address::varchar(256),
        zipcode::numeric(38,0),
        state::varchar(256),
        country::varchar(256),
        convert_timezone('UTC',_fivetran_synced) AS date_load_utc,
        _fivetran_deleted AS date_deleted
    FROM src_addresses
    )

SELECT * FROM renamed_casted
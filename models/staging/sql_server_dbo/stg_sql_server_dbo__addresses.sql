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
        address_id
        , address
        , zipcode
        , state
        , country
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_addresses
    )

SELECT * FROM renamed_casted
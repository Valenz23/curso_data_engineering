{{
    config(
        materialized='view'
    )
}}

with src_promos as (
    select *
    from {{ source("SQL_SERVER_DBO", "PROMOS") }}
),
renamed_casted AS (
    SELECT
        promo_id
        , discount
        , status
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_promos
    )

SELECT * FROM renamed_casted
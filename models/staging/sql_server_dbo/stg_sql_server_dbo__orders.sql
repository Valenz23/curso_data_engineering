{{
    config(
        materialized='view'
    )
}}

with src_orders as (
    select *
    from {{ source("SQL_SERVER_DBO", "ORDERS") }}
),
renamed_casted AS (
    SELECT
        order_id
        , SHIPPING_SERVICE
        , SHIPPING_COST
        , ADDRESS_ID
        , CREATED_AT
        , PROMO_ID
        , ESTIMATED_DELIVERY_AT
        , ORDER_COST
        , USER_ID
        , ORDER_TOTAL
        , DELIVERED_AT
        , TRACKING_ID
        , STATUS
        , _fivetran_deleted AS date_deleted
        , _fivetran_synced AS date_load
    FROM src_orders
    )

SELECT * FROM renamed_casted
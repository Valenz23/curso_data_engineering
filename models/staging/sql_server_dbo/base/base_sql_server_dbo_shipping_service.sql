{{
    config(
        materialized='view'
    )
}}
with source as (
    select * from {{ source('SQL_SERVER_DBO', 'ORDERS') }}

),
renamed as (
    select
        shipping_service,
        _fivetran_deleted,
        _fivetran_synced
    from source
)
select * from renamed
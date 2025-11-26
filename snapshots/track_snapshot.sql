{% snapshot track_snapshot %}

{{
    config(
      unique_key='track_id',
      strategy='check',
      check_cols=['track_type_id', 'track_name'],
    )
}}

with base as ( 
    select * from {{ ref("stg_nascar__track") }}
),
renamed as (
    select
        track_id,
        track_type_id,
        track_name,
        synced_at
    from base
)

select * from renamed

{% endsnapshot %}
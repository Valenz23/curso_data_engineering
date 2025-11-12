{% snapshot users_deletes_snp %}

{{
    config(
      target_schema='snapshots',
      unique_key='DNI',
      strategy='timestamp',
      updated_at='fecha_alta_sistema',
      hard_deletes='new_record',
    )
}}

SELECT
    *
FROM {{ source('google_sheets', 'users') }}

{% endsnapshot %}
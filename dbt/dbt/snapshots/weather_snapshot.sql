{% snapshot weather_snapshot %}

{{
    config(
      target_database='USER_DB_POODLE',  
      target_schema='ANALYTICS',         
      unique_key='forecast_date',

      strategy='check',
      check_cols=['max_temp', 'precipitation', 'wind_speed']
    )
}}

select * from {{ source('raw_data', 'weather_history') }}

{% endsnapshot %}
SELECT
    forecast_date,
    max_temp,
    min_temp,
    precipitation,
    wind_speed
FROM {{ source('raw_data', 'weather_history') }}
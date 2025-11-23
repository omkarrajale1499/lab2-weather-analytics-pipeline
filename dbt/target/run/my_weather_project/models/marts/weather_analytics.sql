
  
    

create or replace transient table USER_DB_POODLE.ANALYTICS.weather_analytics
    
    
    
    as (

WITH source_data AS (
    SELECT * FROM USER_DB_POODLE.ANALYTICS_staging.stg_weather
)

SELECT
    forecast_date,
    max_temp,
    min_temp,
    precipitation,
    wind_speed,
    
    -- Transformation 1: Calculate Temperature Range
    (max_temp - min_temp) as temp_variation,
    
    -- Transformation 2: 7-Day Moving Average of Max Temp (Window Function)
    AVG(max_temp) OVER (
        ORDER BY forecast_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) as seven_day_avg_max_temp,
    
    -- Transformation 3: Categorize Days
    CASE 
        WHEN precipitation > 0 THEN 'Rainy'
        ELSE 'Dry'
    END as rain_status,

    CASE
        WHEN wind_speed > 20 THEN 'High Wind'
        ELSE 'Normal Wind'
    END as wind_status

FROM source_data
    )
;


  
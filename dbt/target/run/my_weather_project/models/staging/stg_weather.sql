
  create or replace   view USER_DB_POODLE.ANALYTICS_staging.stg_weather
  
  
  
  
  as (
    SELECT
    forecast_date,
    max_temp,
    min_temp,
    precipitation,
    wind_speed
FROM USER_DB_POODLE.RAW.weather_history
  );


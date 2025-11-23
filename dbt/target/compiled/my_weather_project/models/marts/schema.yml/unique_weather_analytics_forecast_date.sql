
    
    

select
    forecast_date as unique_field,
    count(*) as n_records

from USER_DB_POODLE.ANALYTICS.weather_analytics
where forecast_date is not null
group by forecast_date
having count(*) > 1



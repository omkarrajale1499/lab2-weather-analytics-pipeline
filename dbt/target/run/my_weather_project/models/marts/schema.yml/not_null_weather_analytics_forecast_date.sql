
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select forecast_date
from USER_DB_POODLE.ANALYTICS.weather_analytics
where forecast_date is null



  
  
      
    ) dbt_internal_test
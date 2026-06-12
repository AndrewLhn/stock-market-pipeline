
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select moving_avg_3d
from "stock_market"."main"."fct_stock_performance"
where moving_avg_3d is null



  
  
      
    ) dbt_internal_test
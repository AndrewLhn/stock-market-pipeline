
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trading_date
from "stock_market"."main"."fct_stock_performance"
where trading_date is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select close_price
from "stock_market"."main"."fct_stock_performance"
where close_price is null



  
  
      
    ) dbt_internal_test
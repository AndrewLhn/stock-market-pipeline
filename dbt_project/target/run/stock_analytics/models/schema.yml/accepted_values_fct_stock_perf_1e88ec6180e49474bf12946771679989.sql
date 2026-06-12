
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        ticker as value_field,
        count(*) as n_records

    from "stock_market"."main"."fct_stock_performance"
    group by ticker

)

select *
from all_values
where value_field not in (
    'AAPL','MSFT','NVDA','TSLA','AMZN','BABA','NFLX','META','AMD','GOOGL'
)



  
  
      
    ) dbt_internal_test

  
    
    

    create  table
      "stock_market"."main"."fct_stock_performance__dbt_tmp"
  
    as (
      

with staging_data as (
    select * from "stock_market"."main"."stg_stocks"
),

calculated_metrics as (
    select
        trading_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        close_price * volume as turnover_usd,
        lag(close_price) over (partition by ticker order by trading_date) as prev_close_price,
        avg(close_price) over (partition by ticker order by trading_date rows between 2 preceding and current row) as moving_avg_3d,
        avg(close_price) over (partition by ticker order by trading_date rows between 6 preceding and current row) as moving_avg_7d
    from staging_data
),

final_data as (
    select
        trading_date,
        ticker,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,
        turnover_usd,
        moving_avg_3d,
        moving_avg_7d,
        case 
            when prev_close_price is not null and prev_close_price > 0 
            then ((close_price - prev_close_price) / prev_close_price) * 100
            else 0 
        end as daily_return_pct
    from calculated_metrics
)

select * from final_data;

INSTALL postgres;
LOAD postgres;
ATTACH 'host=metabase-db user=metabase_user password=metabase_pass_456 dbname=metabase' AS metabase_pg (TYPE POSTGRES);
CREATE OR REPLACE TABLE metabase_pg.public.fct_stock_performance AS SELECT * FROM final_data;
    );
  
  
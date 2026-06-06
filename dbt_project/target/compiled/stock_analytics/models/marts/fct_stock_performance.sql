

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
        
        avg(close_price) over (
            partition by ticker 
            order by trading_date 
            rows between 2 preceding and current row
        ) as moving_avg_3d,

        avg(close_price) over (
            partition by ticker 
            order by trading_date 
            rows between 6 preceding and current row
        ) as moving_avg_7d
    from staging_data
)

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
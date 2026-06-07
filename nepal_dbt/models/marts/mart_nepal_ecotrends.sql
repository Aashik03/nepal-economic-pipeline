with staged as (select * from {{ ref('stg_nepal') }} )
select 
    year,
    gdp_usd,
    gdp_category,
    inflation_pct,
    population,
    exports_pct_gdp,
    imports_pct_gdp,
    trade_balance_pct,
    unemployment_pct,
    round(( 
        (gdp_usd-lag(gdp_usd) over (order by year))
        / lag(gdp_usd) over (order by year)*100
    )::numeric, 2) as gdp_growth_pct,
    round(
        avg(inflation_pct) over(
            order by year
            rows between 4 preceding and current row
        )::numeric, 2
    ) as inflation_5yr_avg
from staged
order by year
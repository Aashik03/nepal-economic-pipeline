with source as (
    select * from {{ source('nepal','nepal_economic_raw') }}
)

select 
    id,country,year,gdp_usd,inflation_pct,population,exports_pct_gdp,imports_pct_gdp,unemployment_pct,ingested_at,

    round((exports_pct_gdp -imports_pct_gdp)::numeric,3) as trade_balance_pct,
        case
            when gdp_usd<1000000000 then 'Low'
            when gdp_usd<3000000000 then 'Developing'
            else 'Emerging'
        end as gdp_category
from source
where year is not NULL
  and gdp_usd is not NULL
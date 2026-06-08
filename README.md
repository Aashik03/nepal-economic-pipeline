# Nepal Economic Indicators Pipeline

An automated data pipeline that extracts 30 years of economic data for Nepal from the [World Bank API](https://data.worldbank.org/), transforms it using pandas and dbt, and loads it into PostgreSQL. Orchestrated end-to-end with Apache Airflow on WSL2.

---

## Pipeline Running Successfully

<img width="1310" height="538" alt="Screenshot 2026-05-31 143630" src="https://github.com/user-attachments/assets/1c5e63a6-ad54-46b0-94c1-f58092e8a9bd" />


<img width="1911" height="794" alt="Screenshot 2026-06-08 133331" src="https://github.com/user-attachments/assets/15490259-c297-4c1a-afc8-49faf7da79ab" />

---

## What It Does

Every week the pipeline:
1. **Extracts** — fetches 6 economic indicators for Nepal from the World Bank API (1995–2024) with retry logic for unstable connections
2. **Transforms** — pivots long-format data into wide-format with pandas, rounds to 3 decimal places, adds derived columns
3. **Loads** — inserts clean records into PostgreSQL with `ON CONFLICT DO NOTHING` for idempotency
4. **dbt run** — transforms raw data into staging and mart models with window functions
5. **dbt test** — validates data quality with 5 automated tests

---

## Indicators Tracked

| Indicator | World Bank Code | Description |
|---|---|---|
| GDP | NY.GDP.MKTP.CD | Gross Domestic Product in USD |
| Inflation | FP.CPI.TOTL.ZG | Consumer price inflation % |
| Population | SP.POP.TOTL | Total population |
| Exports | NE.EXP.GNFS.ZS | Exports as % of GDP |
| Imports | NE.IMP.GNFS.ZS | Imports as % of GDP |
| Unemployment | SL.UEM.TOTL.ZS | Unemployment rate % |

---

## Project Structure

```
nepal-economic-pipeline/
├── extract.py              # World Bank API calls with retry logic
├── transform.py            # pandas pivot, rounding, derived columns
├── load.py                 # PostgreSQL connection and idempotent inserts
├── pipeline.py             # Orchestrates extract → transform → load
├── nepal_economic_dag.py   # Apache Airflow DAG definition
├── nepal_dbt/              # dbt project for transformations and testing
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_nepal.sql              # cleans and standardizes raw data
│   │   │   ├── sources.yml                # points dbt to raw Postgres table
│   │   │   └── schema.yml                 # data quality test definitions
│   │   └── marts/
│   │       └── mart_nepal_ecotrends.sql   # YoY GDP growth, rolling inflation
│   └── dbt_project.yml
├── .env                    # Database credentials (not committed to git)
├── requirements.txt
└── README.md
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11 | Core language |
| requests | World Bank API calls with retry logic |
| pandas | Data cleaning, pivoting, transformation |
| psycopg2 | PostgreSQL connection |
| python-dotenv | Environment variable management |
| Apache Airflow | Pipeline orchestration and weekly scheduling |
| dbt (dbt-postgres) | SQL transformations and data quality testing |
| PostgreSQL | Data storage |

---

## Full Pipeline Architecture

```
World Bank API (6 indicators × 30 years)
              ↓
         extract.py
    (retry logic, 30s timeout)
              ↓
         transform.py
    (pivot long→wide, round 3dp,
     derived columns)
              ↓
PostgreSQL: nepal_economic_raw
              ↓
      dbt: stg_nepal
    (trade_balance_pct, gdp_category)
              ↓
  dbt: mart_nepal_ecotrends
  (YoY GDP growth, 5yr rolling
   average inflation)
```

---

## Orchestration with Apache Airflow

Full pipeline runs as a single DAG with 5 tasks:

```
extract → transform → load → dbt_run → dbt_test
```

### Schedule

Runs `@weekly` — World Bank data updates weekly so hourly runs would be wasteful.

---

## dbt Transformation Layer

### Staging model — `stg_nepal`

Reads from `nepal_economic_raw` and adds:
- `trade_balance_pct` — exports minus imports as % of GDP
- `gdp_category` — classifies economy as low / developing / emerging based on GDP

### Mart model — `mart_nepal_ecotrends`

Built on top of staging using **SQL window functions**:

```sql
-- Year-over-year GDP growth using LAG()
round(
    (
        (gdp_usd - lag(gdp_usd) over (order by year))
        / lag(gdp_usd) over (order by year) * 100
    )::numeric, 2
) as gdp_growth_pct,

-- 5-year rolling average inflation
round(
    avg(inflation_pct) over (
        order by year
        rows between 4 preceding and current row
    )::numeric, 2
) as inflation_5yr_avg
```

### dbt Lineage Graph

<img width="1748" height="831" alt="Screenshot 2026-06-07 200826" src="https://github.com/user-attachments/assets/883bd9d2-3787-4469-8231-89d4d91c1256" />

### Data Quality Tests

| Test | Column | Result |
|---|---|---|
| unique | id | PASS |
| not_null | id | PASS |
| not_null | year | PASS |
| not_null | gdp_usd | PASS |
| not_null | country | PASS |

---

## Setup & Installation

### 1. Clone the repo

```bash
git clone https://github.com/Aashik03/nepal-economic-pipeline.git
cd nepal-economic-pipeline
```

### 2. Create and activate virtual environment

```bash
python -m venv venv
source venv/Scripts/activate   # Windows Git Bash
# or
source venv/bin/activate       # Mac/Linux
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
pip install dbt-postgres
```

### 4. Configure environment variables

Create a `.env` file:
```
DB_HOST=localhost
DB_NAME=nepal_db
DB_USER=postgres
DB_PASSWORD=yourpassword
DB_PORT=5432
```

### 5. Create the PostgreSQL database

```sql
CREATE DATABASE nepal_db;
```

### 6. Run the pipeline once to test

```bash
python pipeline.py
```

### 7. Run dbt manually

```bash
cd nepal_dbt
dbt run
dbt test
```

### 8. Run with Airflow (recommended)

```bash
cp nepal_economic_dag.py ~/airflow/dags/

# Terminal 1
export AIRFLOW_HOME=~/airflow
airflow webserver --port 8080

# Terminal 2
export AIRFLOW_HOME=~/airflow
airflow scheduler
```

Open `http://localhost:8080`, trigger `nepal_economic_pipeline`.

---

## Database Schema

### Raw table

```sql
CREATE TABLE IF NOT EXISTS nepal_economic_raw (
    id               SERIAL PRIMARY KEY,
    country          VARCHAR(50),
    year             INT,
    gdp_usd          FLOAT,
    inflation_pct    FLOAT,
    population       FLOAT,
    exports_pct_gdp  FLOAT,
    imports_pct_gdp  FLOAT,
    unemployment_pct FLOAT,
    ingested_at      TIMESTAMP,
    UNIQUE (country, year)
);
```

### Analytics tables (created by dbt)

```
analytics.stg_nepal              — cleaned staging layer
analytics.mart_nepal_ecotrends   — YoY growth, rolling averages
```

---

## Sample Data in PostgreSQL

<img width="1894" height="808" alt="Screenshot 2026-05-31 144809" src="https://github.com/user-attachments/assets/b1b99a31-3928-4e5d-b0ea-4b9b92d608e9" />


---

## Sample SQL Queries

```sql
-- GDP growth over time
SELECT year, gdp_usd, gdp_growth_pct
FROM analytics.mart_nepal_ecotrends
ORDER BY year;

-- Years with highest inflation
SELECT year, inflation_pct, inflation_5yr_avg
FROM analytics.mart_nepal_ecotrends
ORDER BY inflation_pct DESC
LIMIT 5;

-- Trade balance trend
SELECT year, exports_pct_gdp, imports_pct_gdp, trade_balance_pct
FROM analytics.stg_nepal
ORDER BY year;

-- When did Nepal's economy cross into emerging category?
SELECT year, gdp_usd, gdp_category
FROM analytics.stg_nepal
WHERE gdp_category = 'emerging'
ORDER BY year;
```

---

## Key Engineering Decisions

**Retry logic** — the World Bank API is slow and occasionally times out. Each indicator fetch retries up to 3 times with a 5-second wait between attempts and a 2-second pause between indicators to avoid rate limiting.

**Pivot transform** — raw data comes in long format (one row per indicator per year). pandas `pivot_table()` reshapes it to wide format (one row per year, one column per indicator) — the correct structure for analytical queries.

**Idempotency** — `UNIQUE (country, year)` constraint plus `ON CONFLICT DO NOTHING` means the pipeline can run multiple times safely. No duplicate years ever inserted.

**Window functions in dbt** — `LAG()` for year-over-year comparison and `ROWS BETWEEN 4 PRECEDING AND CURRENT ROW` for rolling averages. These run inside the database where they're fast and maintainable as pure SQL.

**Weekly schedule** — World Bank data updates weekly not hourly. `@weekly` schedule avoids wasteful API calls while keeping data fresh.

---
## License

MIT

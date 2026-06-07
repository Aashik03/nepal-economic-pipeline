import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(dotenv_path=Path(__file__).parent / ".env")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )
def create_table():
    conn=get_connection()
    cur=conn.cursor()
    cur.execute("""
         create table if not exists nepal_economic_raw(
                id serial primary key,
                country varchar(55),
                year int,
                gdp_usd float,
                inflation_pct float,
                population float,
                exports_pct_gdp float,
                imports_pct_gdp float,
                unemployment_pct float,
                ingested_at timestamp,
                unique(country, year)
                );
                """)
    conn.commit()
    cur.close()
    conn.close()
    print("Table created successfully")

def insert_data(df):
        conn=get_connection()
        cur=conn.cursor()
        inserted=0
        for _,row in df.iterrows():
            cur.execute("""
                insert into nepal_economic_raw(
                        country, year, gdp_usd, inflation_pct, population, exports_pct_gdp,
                        imports_pct_gdp, unemployment_pct, ingested_at)
                        values(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict(country, year) do nothing
                        """,(
                            row["country"], row["year"], row.get("gdp_usd"), row.get("inflation_pct"), row.get("population"), row.get("exports_pct_gdp"),row.get("imports_pct_gdp"), row.get("unemployment_pct"), row["ingested_at"]))
            if cur.rowcount>0:
                inserted+=1
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted {inserted} new records")
import pandas as pd

def transform(raw_data):
    df = pd.DataFrame(raw_data)
    df_pivot = df.pivot_table(
        index=["country", "year"],
        columns="indicator_name",
        values="value"
    ).reset_index()
    df_pivot.columns.name = None
    numeric_cols = ["gdp_usd", "inflation_pct", "population", 
                    "exports_pct_gdp", "imports_pct_gdp", "unemployment_pct"]
    for col in numeric_cols:
        if col in df_pivot.columns:
            df_pivot[col] = df_pivot[col].round(3)

    df_pivot["ingested_at"] = pd.Timestamp.now()
    df_pivot = df_pivot.sort_values("year").reset_index(drop=True)

    print(f"Transformed {len(df_pivot)} rows")
    print(df_pivot.head())
    return df_pivot
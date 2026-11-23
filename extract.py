import requests
import urllib3
import time
urllib3.disable_warnings()

base_url="https://api.worldbank.org/v2/country/NP/indicator"
INDICATORS={
    "NY.GDP.MKTP.CD":"gdp_usd",
    "FP.CPI.TOTL.ZG":"inflation_pct",
    "SP.POP.TOTL":"population",
    "NE.EXP.GNFS.ZS":"exports_pct_gdp",
    "NE.IMP.GNFS.ZS":"imports_pct_gdp",
    "SL.UEM.TOTL.ZS":"unemployment_pct",
}

def fetch_indicator(indicator_code, indicator_name, retries=3):
    url = f"{base_url}/{indicator_code}"
    params = {
        "format": "json",
        "per_page": 30,
        "date": "1990:2024"
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=30, verify=False)
            response.raise_for_status()
            data = response.json()
            records = []
            if len(data) > 1 and data[1]:
                for item in data[1]:
                    if item["value"] is not None:
                        records.append({
                            "indicator_code": indicator_code,
                            "indicator_name": indicator_name,
                            "year":           int(item["date"]),
                            "value":          float(item["value"]),
                            "country":        "Nepal"
                        })
            print(f"Fetched {len(records)} records for {indicator_name}")
            return records
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            print(f"Timeout on {indicator_name}, attempt {attempt+1}/{retries}. Retrying...")
            time.sleep(5)  
    raise RuntimeError(f"Failed to fetch {indicator_name} after {retries} attempts")


def extract_all():
    all_records = []
    for code, name in INDICATORS.items():
        records = fetch_indicator(code, name)
        all_records.extend(records)
        time.sleep(2)
    print(f"Total: {len(all_records)} records extracted")
    return all_records

#extract_all()
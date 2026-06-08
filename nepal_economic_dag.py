from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/root/nepal-economic-pipeline")

from extract import extract_all
from transform import transform
from load import create_table, insert_data

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

def run_extract(**context):
    raw = extract_all()
    context["ti"].xcom_push(key="raw_data", value=raw)

def run_transform(**context):
    raw = context["ti"].xcom_pull(key="raw_data", task_ids="extract")
    df = transform(raw)
    # convert timestamp to string so XCom can serialize it
    df["ingested_at"] = df["ingested_at"].astype(str)
    context["ti"].xcom_push(key="clean_data", value=df.to_dict("records"))

def run_load(**context):
    import pandas as pd
    records = context["ti"].xcom_pull(key="clean_data", task_ids="transform")
    df = pd.DataFrame(records)
    df["ingested_at"] = pd.to_datetime(df["ingested_at"])
    create_table()
    insert_data(df)

with DAG(
    dag_id="nepal_economic_pipeline",
    default_args=default_args,
    description="Nepal economic indicators from World Bank API",
    schedule_interval="@weekly",   # economic data updates weekly
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["nepal", "economics", "worldbank"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=run_extract)
    t_transform = PythonOperator(task_id="transform", python_callable=run_transform)
    t_load = PythonOperator(task_id="load", python_callable=run_load)

    t_dbt_run = BashOperator(
    	task_id="dbt_run",
    	bash_command="cd /root/nepal-economic-pipeline/nepal_dbt && dbt run --profiles-dir /root/.dbt",
    )

    t_dbt_test = BashOperator(
    	task_id="dbt_test",
    	bash_command="cd /root/nepal-economic-pipeline/nepal_dbt && dbt test --profiles-dir /root/.dbt",
    )

    t_extract >> t_transform >> t_load >> t_dbt_run >> t_dbt_test
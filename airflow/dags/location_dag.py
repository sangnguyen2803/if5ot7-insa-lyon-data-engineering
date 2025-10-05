# libraries
from datetime import timedelta
import pandas as pd
import pendulum, requests, json, random, glob, logging

from airflow import DAG

# operators
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

START_DATE = pendulum.datetime(2025, 10, 5, tz="UTC")

with DAG(
    dag_id="location_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5)
    },
    tags="location_dag"
) as dag:
    def fetch_location(api_url: str, output_folder: str):
        resp = requests.get(api_url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        location = data.get("iss_position", {})
        with open(f"{output_folder}/task_1.json", "w", encoding="utf-8") as f:
            json.dump({"location": location}, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved location: {location}")

    first_node = PythonOperator(
        task_id="fetch_location",
        python_callable=fetch_location,
        op_kwargs={
            "api_url": "https://api.open-notify.org/iss-now.json",
            "output_folder": "/opt/airflow/dags"
        }
    )
    first_node
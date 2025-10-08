import pendulum
import pandas as pd
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)  # Airflow captures this per task
output_folder = "/opt/airflow/data" # ensure this folder exists and is writable

# --- DAG config ---
START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")
OLTP_CONN_ID = "postgres_default"

with DAG(
    dag_id="migration_dag",
    start_date=START_DATE,
    schedule="0 0 * * *",
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    
    template_searchpath=["/opt/airflow/data/"],
) as dag:
    first_node = SQLExecuteQueryOperator(
        task_id="init_oltp_schema",
        conn_id="postgres_default",
        sql="create_table.sql",
    )

    second_node = SQLExecuteQueryOperator(
        task_id="insert_data_schema",
        conn_id="postgres_default",
        sql="data.sql",
    )
    # -------- Graph --------
    first_node >> second_node

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="first_dag_stub",
    start_date=START_DATE,
    schedule=None,          # no schedule
    catchup=False,
    max_active_tasks=1,     # old 'concurrency'
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stub"],
) as dag:

    get_spreadsheet = BashOperator(
        task_id="get_spreadsheet",
        bash_command="curl https://www.lutemusic.org/spreadsheet.xlsx --output /opt/airflow/dags/{{ds_nodash}}.xlsx",  
    )
    transmute_to_csv = BashOperator(
        task_id="transmute_to_csv",
        trigger_rule="all_success",
        bash_command="xlsx2csv /opt/airflow/dags/{{ds_nodash}}.xlsx > /opt/airflow/dags/{{ds_nodash}}_converted.csv"
    )
    time_filter = EmptyOperator(
        task_id="time_filter",
        trigger_rule="all_success",
        #bash_command="awk -F, '$31 > 1588612377' /opt/airflow/dags/{{ds_nodash}}_converted.csv > /opt/airflow/dags/{{ds_nodash}}_converted_filtered.csv"
    )
    load = BashOperator(
        task_id="load",
        trigger_rule="all_success",
        bash_command="echo \"loaded\"",
    )
    cleanup = EmptyOperator(
        task_id="cleanup"
    )

    get_spreadsheet >> transmute_to_csv >> time_filter >> load >> cleanup

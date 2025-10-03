import pendulum
from datetime import timedelta
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

with DAG(
    dag_id="second_dag_stub",
    start_date=START_DATE,
    schedule="0 0 * * *",   # daily at 00:00 UTC
    catchup=False,
    max_active_tasks=1,     # replaces old DAG-level 'concurrency'
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/dags/"],
    tags=["stub"],
) as dag:
    def check_emptiness(file_name: str):
        df = pd.read_csv(f"{file_name}")
        return "end" if df.empty else "split"
    
    def split_data(file_name: str, output_name: str):
        df = pd.read_csv(f"{file_name}")
        df = df.replace(',|"|\'|`', "", regex=True)
        df_intavolature = df[["Piece", "Type", "Key", "Difficulty", "Date", "Ensemble"]]
        df_composer = df[["Composer"]].drop_duplicates()
        df_intavolature.to_csv(f"{output_name}_intavolature.csv", index=False)
        df_composer.to_csv(f"{output_name}_composer.csv", index=False)
    
    def create_composer_query(file_name: str):
        df = pd.read_csv(f"{file_name}")
        with open("/opt/airflow/dags/composer_inserts.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS composer (\n"
                " name VARCHAR(255)\n"
                ");\n"
            )
            for _, row in df.iterrows():
                composer = row.get("Composer", "")
                f.write(f"INSERT INTO composer VALUES ('{composer}');\n")
    
    def create_intavolature_query(file_name: str):
        df = pd.read_csv(f"{file_name}")
        with open("/opt/airflow/dags/intavolature_inserts.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS intavolature (\n"
                "  title VARCHAR(255),\n"
                "  subtitle VARCHAR(255),\n"
                "  key VARCHAR(255),\n"
                "  difficulty VARCHAR(255),\n"
                "  date VARCHAR(255),\n"
                "  ensemble VARCHAR(255)\n"
                ");\n"
            )
            for _, row in df.iterrows():
                piece = row.get("Piece", "")
                type_ = row.get("Type", "")
                key = row.get("Key", "")
                difficulty = row.get("Difficulty", "")
                date = row.get("Date", "")
                ensemble = row.get("Ensemble", "")
                f.write(
                    "INSERT INTO intavolature VALUES ("
                    f"'{piece}', '{type_}', '{key}', '{difficulty}', '{date}', '{ensemble}'"
                    ");\n"
                )
    get_spreadsheet = BashOperator(
        task_id="get_spreadsheet",
        bash_command="curl https://www.lutemusic.org/spreadsheet.xlsx --output /opt/airflow/dags/{{ds_nodash}}.xlsx",
    )
    transmute_to_csv = BashOperator(
        task_id="transmute_to_csv",
        trigger_rule="all_success",
        bash_command="xlsx2csv /opt/airflow/dags/{{ds_nodash}}.xlsx > /opt/airflow/dags/{{ds_nodash}}_converted.csv"
    )
    time_filter = BashOperator(
        task_id="time_filter",
        trigger_rule="all_success",
        bash_command="awk -F, '$31 > 1588612377' /opt/airflow/dags/{{ds_nodash}}_converted.csv > /opt/airflow/dags/{{ds_nodash}}_converted_filtered.csv"
    )

    emptiness_check = BranchPythonOperator(
        task_id="emptiness_check",
        python_callable=check_emptiness,
        op_kwargs={
            "file_name": "/opt/airflow/dags/{{ds_nodash}}_converted_filtered.csv"
        }
    )
    split = PythonOperator(
        task_id="split",
        python_callable=split_data,
        op_kwargs={
            "file_name": "/opt/airflow/dags/{{ds_nodash}}_converted.csv",
            "output_name": "/opt/airflow/dags/{{ds_nodash}}"
        }
    )

    create_intavolature_query = PythonOperator(
        task_id="create_intavolature_query",
        python_callable=create_intavolature_query,
        op_kwargs={
            "file_name": "/opt/airflow/dags/{{ds_nodash}}_intavolature.csv"
        }
    )
    
    create_composer_query = PythonOperator(
        task_id="create_composer_query",
        python_callable=create_composer_query,
        op_kwargs={
            "file_name": "/opt/airflow/dags/{{ds_nodash}}_composer.csv"
        }
    )


    insert_intavolature_query = SQLExecuteQueryOperator(
        task_id="insert_intavolature_query",
        conn_id="postgres_default",
        sql="intavolature_inserts.sql",
        autocommit=True,
    )
    
    insert_composer_query = SQLExecuteQueryOperator(
        task_id="insert_composer_query",
        conn_id="postgres_default",
        sql="composer_inserts.sql",
        autocommit=True,
    )

    join_tasks = EmptyOperator(
        task_id="coalesce_transformations",
        trigger_rule="none_failed"
    )
    end = EmptyOperator(
        task_id="end",
        trigger_rule="none_failed"
    )

    # Graph (structure preserved)
    get_spreadsheet >> transmute_to_csv >> time_filter >> emptiness_check
    emptiness_check >> [split, end]
    split >> [create_intavolature_query, create_composer_query]
    create_intavolature_query >> insert_intavolature_query
    create_composer_query >> insert_composer_query
    [insert_intavolature_query, insert_composer_query] >> join_tasks >> end
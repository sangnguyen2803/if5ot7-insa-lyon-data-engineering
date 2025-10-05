# libraries
from datetime import timedelta
import pandas as pd
import pendulum, requests, json, random, glob, logging

from airflow import DAG

# operators
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
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

        
    def find_closest_country(output_folder: str):
        # load ISS location
        with open(f"{output_folder}/task_1.json") as f:
            iss_pos = json.load(f)["location"]
        
        # dummy list of countries with lat/lon (replace with actual coordinates)
        countries = [
            {"name": "USA", "lat": 38.0, "lon": -97.0, "continent": "North America"},
            {"name": "France", "lat": 46.0, "lon": 2.0, "continent": "Europe"},
            {"name": "Japan", "lat": 36.0, "lon": 138.0, "continent": "Asia"},
        ]
        
        iss_coords = (float(iss_pos["latitude"]), float(iss_pos["longitude"]))
        
        # find closest country
        closest = min(countries, key=lambda c: geodesic(iss_coords, (c["lat"], c["lon"])).km)
        
        with open(f"{output_folder}/task_2.json", "w") as f:
            json.dump(closest, f, ensure_ascii=False)

    def output_continent(output_folder: str):
        with open(f"{output_folder}/task_2.json") as f:
            data = json.load(f)
        
        with open(f"{output_folder}/task_4.json", "w") as f:
            json.dump({"continent": data["continent"]}, f, ensure_ascii=False)


    first_node = PythonOperator(
        task_id="fetch_location",
        python_callable=fetch_location,
        op_kwargs={
            "api_url": "https://api.open-notify.org/iss-now.json",
            "output_folder": "/opt/airflow/dags"
        }
    )
    second_node = PythonOperator(
        task_id="find_closest_country",
        python_callable=find_closest_country,
        op_kwargs={"output_folder": "/opt/airflow/dags"}
    )
    third_node = PostgresOperator(
        task_id="save_country_to_db",
        postgres_conn_id="your_postgres_conn_id",
        sql="""
        INSERT INTO iss_closest_country (name, lat, lon, continent)
        SELECT name, lat, lon, continent
        FROM json_populate_recordset(NULL::iss_closest_country, 
            '[{"name": "France", "lat":46, "lon":2, "continent":"Europe"}]');
        """,
        autocommit=True
    )
    fourth_node = PythonOperator(
        task_id="output_continent",
        python_callable=output_continent,
        op_kwargs={"output_folder": "/opt/airflow/dags"}
    )

    first_node >> second_node >> third_node >> fourth_node


    
# libraries
from datetime import timedelta
import pandas as pd
import pendulum, requests, json, random, glob

from airflow import DAG

# operators
from airflow.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

START_DATE = pendulum.datetime(2025, 9, 30, tz="UTC")

with DAG(
    dag_id="assignment_dag_real",
    start_date=START_DATE,
    schedule="0 0 * * *",
    catchup=False,
    max_active_tasks=1,
    default_args={
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=['/opt/airflow/dags/'],
    tags="assignment",
) as dag:
    def fetch_name_race(api_url: str, endpoint: str, output_folder: str):
        resp = requests.get(f"{api_url}/{endpoint}", timeout=30)
        resp.raise_for_status()
        race = resp.json().get("results",[])
        if not race:
            raise ValueError("No races returned from API")
        races = random.sample(race, 5)
        payload = {"race": races}
        with open(f"{output_folder}/first_node.json", "w") as f:
            json.dump(payload, f, ensure_ascii=False)

    def attributes(output_folder: str):
        attributes = [list(random.randint(6,19) for _ in range(5)) for _ in range(5)]
        print(attributes)
        with open(f"{output_folder}/second_note.json", "w") as f:
            json.dump({"attributes": attributes}, f, ensure_ascii=False)

    def language(api_url: str, endpoint: str, output_folder: str):
        resp = requests.get(f"{api_url}/{endpoint}", timeout=30)
        resp.raise_for_status()
        language = resp.json().get("results", [])
        if not language:
            raise ValueError("No languages returned from API")
        languages = random.sample(language, 5)
        payload = {"language": languages}
        with open(f"{output_folder}/third_node.json", "w") as f:
            json.dump(payload, f, ensure_ascii=False)

    def classes(api_url: str, endpoint: str, output_folder: str):
        resp = requests.get(f"{api_url}/{endpoint}", timeout=30)
        resp.raise_for_status()
        classes = resp.json().get("results", [])
        if not classes:
            raise ValueError("No classes returned from API")
        classes = random.sample(classes, 5)
        payload = {"classes": classes}
        with open(f"{output_folder}/fourth_node.json", "w") as f:
            json.dump(payload, f, ensure_ascii=False)

    def proficiency_choices(output_folder: str, url: str):
        with open(f"{output_folder}/fourth_node.json", "r") as rf:
            load_classes = json.load(rf)
        classes = load_classes.get("class", [])
        
        def get_proficiencies(cls):
            d = requests.get(f"{url}/classes/{cls}", timeout=30)
            first_choice = (d.get("proficiency_choices") or [{}])[0]
            options = (first_choice.get("from").get("options") or [])   # <-- fixed name
            choose_n = int(first_choice.get("choose") or 0)
            picked = random.sample([i.get("item").get("index") for i in options], choose_n) if choose_n and options else []
            return picked
        
        final_list = [get_proficiencies(c) for c in classes]
        with open(f"{output_folder}/fifth_node.json", "w") as f:
            json.dump({"proficiences": final_list}, f, ensure_ascii=False)

    def levels(output_folder: str):
        random_levels = [random.randint(1, 3) for _ in range(5)]
        with open(f"{output_folder}/sixth_node.json", "w") as f:
            json.dump({"levels": random_levels}, f, ensure_ascii=False)
    
    def spell_check(output_folder: str):
        with open(f"{output_folder}/fourth_node.json", "r") as read_file:
            load_classes = json.load(read_file)
        classes = load_classes.get("class", [])
        def spellcount(cls):
            resp = requests.get(f"https://www.dnd5eapi.co/api/classes/{cls}/spells", timeout=30).json()
            return int(resp.get("count") or 0)
        total = sum(spellcount(c) for c in classes)
        return "spells" if total > 0 else "merge"

    def spells(output_folder: str, url: str):
        with open(f"{output_folder}/fourth_node.json", "r") as read_file:
            load_classes = json.load(read_file)
        classes = load_classes.get("class", [])
        def get_spells(cls):
            resp = requests.get(f"{url}/classes/{cls}/spells", timeout=30).json()
            results = resp.get("results", [])
            pick = random.randint(1, min(3, len(results))) if results else 0
            chosen = random.sample([i.get("index") for i in results], pick) if pick else []
            return chosen
        spell_lists = [get_spells(c) for c in classes]
        with open(f"{output_folder}/seventh_node.json", "w") as f:
            json.dump({"spells": spell_lists}, f, ensure_ascii=False)

    def merge(output_folder: str):
        jsons = sorted(glob.glob(f"{output_folder}/*_node.json"))
        dfs = [pd.read_json(p) for p in jsons]
        df = pd.concat(dfs, axis=1) if dfs else pd.DataFrame()
        df.to_csv(f"{output_folder}/eight_node.csv", index=False)

    def insert(output_folder: str):
        df = pd.read_csv(f"{output_folder}/eight_node.csv")
        with open(f"{output_folder}/inserts.sql", "w") as f:
            f.write(
                "CREATE TABLE IF NOT EXISTS characters (\n"
                "  race TEXT,\n"
                "  name TEXT,\n"
                "  proficiences TEXT,\n"
                "  language TEXT,\n"
                "  spells TEXT,\n"
                "  class TEXT,\n"
                "  levels TEXT,\n"
                "  attributes TEXT\n"
                ");\n"
            )
            for _, row in df.iterrows():
                race = row.get("race", "")
                name = row.get("name", "")
                proficiences = row.get("proficiences", "")
                language = row.get("language", "")
                spells = row.get("spells", "")
                class_ = row.get("class", "")
                levels = row.get("levels", "")
                attributes = row.get("attributes", "")
                f.write(
                    "INSERT INTO characters VALUES ("
                    f"'{race}', '{name}', $${proficiences}$$, '{language}', $${spells}$$, '{class_}', '{levels}', $${attributes}$$"
                    ");\n"
                )

    first_node = PythonOperator(
        task_id="name_race",
        python_callable=fetch_name_race,
        op_kwargs={
            "api_url": "https://www.dnd5eapi.co/api",
            "endpoint": "races",
            "output_folder": "/opt/airflow/dags"
        }
    )
    second_node = PythonOperator(
        task_id="attributes",
        python_callable=attributes,
        op_kwargs={
            "output_folder": "/opt/airflow/dags"
        },
    )
    third_node = PythonOperator(
        task_id="language",
        python_callable=language,
        op_kwargs={
            "api_url": "https://www.dnd5eapi.co/api",
            "output_folder": "/opt/airflow/dags",
            "endpoint": "languages",
        }
    )
    fourth_node = PythonOperator(
        task_id="classes",
        python_callable=classes,
        op_kwargs={
            "api_url": "https://www.dnd5eapi.co/api",
            "output_folder": "/opt/airflow/dags",
            "endpoint": "classes",
        }
    )
    fifth_node = PythonOperator(
        task_id="proficiency_choices",
        python_callable=proficiency_choices,
        op_kwargs={
            "output_folder": "/opt/airflow/dags", 
            "url": "https://www.dnd5eapi.co/api"
        }
    )
    sixth_node = PythonOperator(
        task_id="levels",
        python_callable=levels,
        op_kwargs={
            "output_folder": "/opt/airflow/dags"
        },
    )
    seventh_node = PythonOperator(
        task_id="spell_check",
        python_callable=spell_check,
        op_kwargs={
            "output_folder": "/opt/airflow/dags",
            "url": "https://www.dnd5eapi.co/api",
        }
    )
    seventh_node_a = PythonOperator(
        task_id="spells",
        python_callable=spells,
        op_kwargs={
            "output_folder": "/opt/airflow/dags", 
            "url": "https://www.dnd5eapi.co/api",
        }
    )

    eight_node = PythonOperator(
        task_id="merge",
        python_callable=merge,
        op_kwargs={
            "output_folder": "/opt/airflow/dags"
        }
    )

    ninth_node = PythonOperator(
        task_id="generate_insert",
        python_callable=insert,
        op_kwargs={
            "output_folder": "/opt/airflow/dags"
        }
    )

    tenth_node = SQLExecuteQueryOperator(
        task_id="insert_inserts",
        conn_id="postgres_default",
        sql="inserts.sql",
        autocommit=True
    )

    [first_node, second_node, third_node, fourth_node] >> fifth_node
    fifth_node >> sixth_node >> seventh_node >> [seventh_node_a, eight_node]
    seventh_node_a >> eight_node >> ninth_node >> tenth_node

import airflow
import faker
import json
import logging
import pendulum
import random
from datetime import timedelta
import pandas as pd
import requests
import os
from glob import glob

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

logger = logging.getLogger(__name__)

START_DATE = pendulum.datetime(2020, 6, 25, tz="UTC")
ENDPOINT = 'https://www.dnd5eapi.co/api/2014'
JSON_FOLDER = '/opt/airflow/data/dnd/json'
CSV_FOLDER = '/opt/airflow/data/dnd/csv'
SQL_FOLDER = '/opt/airflow/data/dnd/sql'

os.makedirs(JSON_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(SQL_FOLDER, exist_ok=True)

with DAG(
    dag_id="assignment_dag",
    start_date=START_DATE,
    schedule="0 16 * * 5",   # weekly at 16:00 UTC
    catchup=False,
    max_active_tasks=1,     # replaces old DAG-level 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    template_searchpath=["/opt/airflow/data/dnd/sql"],
    tags=["assignment"],
) as dag:

    def _classes(filename: str, url: str, output_folder: str):
        response = requests.get(url=url)
        resp = response.json()
        values = random.sample(resp.get("results", []), 5)
        with open(f"{output_folder}/{filename}.json", "w") as f:
            json.dump({"classes": [v.get("index") for v in values]}, f)

    def _races(filename: str, url: str, output_folder: str):
        response = requests.get(url=url)
        resp = response.json()
        values = random.sample(resp.get("results", []), 5)
        with open(f"{output_folder}/{filename}.json", "w") as f:
            json.dump({"races": [v.get("index") for v in values]}, f)

    def _languages(filename: str, url: str, output_folder: str):
        response = requests.get(url=url)
        resp = response.json()
        values = random.sample(resp.get("results", []), 5)
        with open(f"{output_folder}/{filename}.json", "w") as f:
            json.dump({"languages": [v.get("index") for v in values]}, f)

    def _spells(output_folder: str):
        with open(f"{output_folder}/classes.json", "r") as f:
            classes = json.load(f).get("classes", [])

        with open(f"{output_folder}/levels.json", "r") as f:
            levels = json.load(f).get("levels", [])

        spells = []
        for i in range(len(classes)):
            index = classes[i]
            level = levels[i]
            response = requests.get(url=f'{ENDPOINT}/classes/{index}/spells')
            resp = response.json()
            if resp.get("count", 0) < level + 3:
                values = resp.get("results", [])
            else:
                values = random.sample(resp.get("results", []), level + 3)
            spells.append(values)

        logger.info(spells)
        with open(f"{output_folder}/spells.json", "w") as f:
            json.dump({"spells": [[s.get("index") for s in spell_list] for spell_list in spells]}, f)

    def _proficiencies(output_folder: str):
        with open(f"{output_folder}/classes.json", "r") as f:
            classes = json.load(f).get("classes")

        proficiencies = []
        for i in range(len(classes)):
            index = classes[i]
            response = requests.get(url=f'{ENDPOINT}/classes/{index}/proficiencies')
            resp = response.json()
            if resp.get("count", 0) < 1:
                values = resp.get("results", [])
            else:
                values = random.sample(resp.get("results", []), 1)
            proficiencies.append(values)

        with open(f"{output_folder}/proficiencies.json", "w") as f:
            json.dump({"proficiencies": [[p.get("index") for p in proficiencies_list] for proficiencies_list in proficiencies]}, f)

    def _names(output_folder: str):
        names = [faker.Faker().first_name() for _ in range(5)]
        with open(f"{output_folder}/names.json", "w") as f:
            json.dump({"name": names}, f)

    def _attributes(output_folder: str):
        attributes = [[random.randint(2, 18) for _ in range(6)] for _ in range(5)]
        with open(f"{output_folder}/attributes.json", "w") as f:
            json.dump({"attributes": attributes}, f)

    def _levels(output_folder: str):
        levels = [random.randint(1, 3) for _ in range(5)]
        with open(f"{output_folder}/levels.json", "w") as f:
            json.dump({"levels": levels}, f)

    def _merge(input_folder: str, output_folder: str):        
        data = [pd.read_json(f) for f in glob(f"{input_folder}/*.json")]
        logger.info(f"Merged data: {data}")
        df = pd.concat(data, axis=1)
        logger.info(df)
        df.to_csv(f"{output_folder}/characters.csv", index=False)

    def _create_query():
        query = """
        CREATE TABLE IF NOT EXISTS characters (
            name VARCHAR(255),
            class VARCHAR(255),
            race VARCHAR(255),
            proficiencies VARCHAR(255),
            language VARCHAR(255),
            spells VARCHAR(255),
            levels VARCHAR(255),
            attributes VARCHAR(255)
        );
        """

        df = pd.read_csv(f"{CSV_FOLDER}/characters.csv")
        for _, row in df.iterrows():
            query += f"""
        INSERT INTO characters VALUES ('{row["name"]}', '{row["classes"]}', '{row["races"]}', $$'{row["proficiencies"]}'$$, '{row["languages"]}', $$'{row["spells"]}'$$, '{row["levels"]}', $$'{row["attributes"]}'$$);
            """
        with open(f"{SQL_FOLDER}/create_and_insert_characters.sql", "w") as f:
            f.write(query)

    classes = PythonOperator(
        task_id="classes",
        python_callable=_classes,
        op_kwargs={
            "output_folder": JSON_FOLDER,
            "filename": "classes",
            "url": f"{ENDPOINT}/classes",
        },
    )

    races = PythonOperator(
        task_id="races",
        python_callable=_races,
        op_kwargs={
            "output_folder": JSON_FOLDER,
            "filename": "races",
            "url": f"{ENDPOINT}/races",
        },
    )

    languages = PythonOperator(
        task_id="languages",
        python_callable=_languages,
        op_kwargs={
            "output_folder": JSON_FOLDER,
            "filename": "languages",
            "url": f"{ENDPOINT}/languages",
        },
    )

    proficiencies = PythonOperator(
        task_id="proficiencies",
        python_callable=_proficiencies,
        op_kwargs={
            "output_folder": JSON_FOLDER,
        },
    )

    spells = PythonOperator(
        task_id="spells",
        python_callable=_spells,
        op_kwargs={
            "output_folder": JSON_FOLDER,
        },
    )

    names = PythonOperator(
        task_id="names",
        python_callable=_names,
        op_kwargs={
            "output_folder": JSON_FOLDER,
        },
    )

    attributes = PythonOperator(
        task_id="attributes",
        python_callable=_attributes,
        op_kwargs={
            "output_folder": JSON_FOLDER,
        },
    )

    levels = PythonOperator(
        task_id="levels",
        python_callable=_levels,
        op_kwargs={
            "output_folder": JSON_FOLDER,
            "filename": "levels",
        },
    )



    merge = PythonOperator(
        task_id="merge",
        python_callable=_merge,
        op_kwargs={
            "input_folder": JSON_FOLDER,
            "output_folder": CSV_FOLDER,
        },
    )

    create_query = PythonOperator(
        task_id="create_query",
        python_callable=_create_query,
    )

    execute_query = SQLExecuteQueryOperator(
        task_id="execute_query",
        conn_id="postgres_default",
        sql=f"create_and_insert_characters.sql",
        autocommit=True,
    )

    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_success",
    )

# Graph (structure preserved)
[names, classes, attributes, races, languages, levels] >> spells
spells >> proficiencies
proficiencies >> merge
merge >> create_query
create_query >> execute_query
execute_query >> end
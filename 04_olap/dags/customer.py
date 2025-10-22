from __future__ import annotations
import json
import os

import pendulum
import pandas as pd
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import asyncio
import asyncpg
from airflow.models import BaseOperator
from airflow.providers.http.sensors.http import HttpSensor
from operators.postgres_event import WaitForPostgresEventOperator
import logging

logger = logging.getLogger(__name__)

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")
JSON_FOLDER = '/opt/airflow/data/json'
CSV_FOLDER = '/opt/airflow/data/csv'
SQL_FOLDER = '/opt/airflow/data/sql'

os.makedirs(JSON_FOLDER, exist_ok=True)
os.makedirs(CSV_FOLDER, exist_ok=True)
os.makedirs(SQL_FOLDER, exist_ok=True)

with DAG(
    dag_id="customer",
    start_date=START_DATE,
    schedule=None,          # no schedule
    catchup=False,
    max_active_tasks=1,     # old 'concurrency'
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["stub"],
    template_searchpath=[SQL_FOLDER],
) as dag:

    def process_customer_changes():
        logger.info(f"Processing customer changes: {JSON_FOLDER}/DimCustomer.json")
        with open(f"{JSON_FOLDER}/DimCustomer.json", "r") as f:
            event = json.load(f)
        logger.info(f"Processing event: {event}")
        operation = event["op"]
        if operation == "INSERT":
            logger.info("Inserting row...")
            return "insert"
        elif operation == "UPDATE":
            logger.info("Updating row...")
            return "update"
        elif operation == "DELETE":
            logger.info("Deleting row...")
            return "delete"
        
    def insert_customer():
        with open(f"{JSON_FOLDER}/DimCustomer.json", "r") as f:
            event = json.load(f)
        row = event["data"]

        with open(f"{SQL_FOLDER}/db2/DimCustomer.sql", "w") as file:
            file.write(
                "INSERT INTO DimCustomer (CustomerKey, FirstName, LastName, Segment, City, ValidFrom, ValidTo)\n"
                f"VALUES ({row['customerid']}, '{row['firstname']}', '{row['lastname']}', '', '', '{datetime.now().strftime('%Y-%m-%d')}', '9999-12-31');"
            )

    def update_customer():
        with open(f"{JSON_FOLDER}/DimCustomer.json", "r") as f:
            event = json.load(f)
        row = event["data"]

        with open(f"{SQL_FOLDER}/db2/DimCustomer.sql", "w") as file:
            file.write(
                "UPDATE DimCustomer"
                f" SET ValidTo = '{datetime.now().strftime('%Y-%m-%d')}'"
                f" WHERE CustomerKey = {row['customerid']} AND ValidTo = '9999-12-31';"
                f"\n"
                f"INSERT INTO DimCustomer (CustomerKey, FirstName, LastName, Segment, City, ValidFrom, ValidTo)\n"
                f"VALUES ({row['customerid']}, '{row['firstname']}', '{row['lastname']}', '', '', '{datetime.now().strftime('%Y-%m-%d')}', '9999-12-31');"
            )

    def delete_customer():
        with open(f"{JSON_FOLDER}/DimCustomer.json", "r") as f:
            event = json.load(f)
        row = event["data"]

        with open(f"{SQL_FOLDER}/db2/DimCustomer.sql", "w") as file:
            file.write(
                "UPDATE DimCustomer"
                f" SET ValidTo = '{datetime.now().strftime('%Y-%m-%d')}'"
                f" WHERE CustomerKey = {row['customerid']} AND ValidTo = '9999-12-31';"
            )


    wait_for_event = WaitForPostgresEventOperator(
        task_id="wait_for_db_event",
        conn_str="postgresql://data_engineer:Pass!w0rd@db1:5432/assignment",
        channel="airflow_event",
        table="DimCustomer",
    )

    process_changes = BranchPythonOperator(
        task_id="process_changes",
        python_callable=process_customer_changes,
    )

    insert_query = PythonOperator(
        task_id="insert",
        python_callable=insert_customer,
    )

    update_query = PythonOperator(
        task_id="update",
        python_callable=update_customer,
    )

    delete_query = PythonOperator(
        task_id="delete",
        python_callable=delete_customer,
    )

    execute_query = SQLExecuteQueryOperator(
        task_id="execute_query",
        trigger_rule="none_failed",
        conn_id="db2",
        sql=f"db2/DimCustomer.sql",
    )

    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id="customer",
        wait_for_completion=False,
    )

    wait_for_event >> process_changes
    process_changes >> [insert_query, update_query, delete_query] >> execute_query
    execute_query >> restart_dag


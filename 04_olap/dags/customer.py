from __future__ import annotations

import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator

from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import asyncio
import asyncpg
from airflow.models import BaseOperator
from airflow.providers.http.sensors.http import HttpSensor
from operators.postgres_event import WaitForPostgresEventOperator
import logging

logger = logging.getLogger(__name__)

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")

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
) as dag:

    def process_customer_changes(ti, **kwargs):
        event = ti.xcom_pull(task_ids='wait_for_db_event', key='db_event')
        logger.info(f"Processing event: {event}")
        operation = event["payload"]
        if operation == "insert":
            logger.info("Row inserted")
            return "insert"
        elif operation == "update":
            logger.info("Row updated")
            return "update"
        elif operation == "delete":
            logger.info("Row deleted")
            return "delete"

    wait_for_event = WaitForPostgresEventOperator(
        task_id="wait_for_db_event",
        conn_str="postgresql://data_engineer:Pass!w0rd@db1:5432/assignment",
        channel="airflow_event",
    )

    process_changes = BranchPythonOperator(
        task_id="process_changes",
        python_callable=process_customer_changes,
    )

    insert_query = EmptyOperator(
        task_id="insert",
    )

    update_query = EmptyOperator(
        task_id="update",
    )

    delete_query = EmptyOperator(
        task_id="delete",
    )

    execute_query = EmptyOperator(
        task_id="execute_query",
        trigger_rule="none_failed",
    )

    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id="customer",
        wait_for_completion=False,
    )

    wait_for_event >> process_changes
    process_changes >> [insert_query, update_query, delete_query] >> execute_query
    execute_query >> restart_dag


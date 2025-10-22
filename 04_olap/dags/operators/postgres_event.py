import json
from airflow.models import BaseOperator
from airflow.triggers.base import TriggerEvent
from airflow.exceptions import AirflowSkipException

class WaitForPostgresEventOperator(BaseOperator):
    def __init__(self, conn_str: str, channel: str, table: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_str = conn_str
        self.channel = channel
        self.table = table

    def execute(self, context):
        from triggers.postgres_notify import PostgresNotifyTrigger
        self.defer(
            trigger=PostgresNotifyTrigger(conn_str=self.conn_str, channel=self.channel),
            method_name="resume_after_event"
        )

    def resume_after_event(self, context, event: TriggerEvent = None):
        if event:
            self.log.info(f"Received event: {event}; type: {type(event)}")
            self.log.info(f"Writing event to /opt/airflow/data/json/{self.table}.json")
            with open(f'/opt/airflow/data/json/{self.table}.json', "w") as f:
                json.dump(event, f)
        else:
            raise AirflowSkipException("No event received")

import asyncio
import asyncpg
from airflow.triggers.base import BaseTrigger, TriggerEvent

class PostgresNotifyTrigger(BaseTrigger):
    def __init__(self, conn_str: str, channel: str):
        super().__init__()
        self.conn_str = conn_str
        self.channel = channel

    def serialize(self):
        return (
            "triggers.postgres_notify.PostgresNotifyTrigger",
            {"conn_str": self.conn_str, "channel": self.channel},
        )

    async def run(self):
        conn = await asyncpg.connect(self.conn_str)
        await conn.add_listener(self.channel, self._on_notify)
        self.event = None
        while not self.event:
            await asyncio.sleep(1)
        await conn.close()
        yield TriggerEvent(self.event)

    def _on_notify(self, connection, pid, channel, payload):
        self.event = {"payload": payload}

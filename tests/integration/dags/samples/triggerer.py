from airflow import version
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from airflow import settings
from airflow.models import Variable

from datetime import datetime

import time
from datetime import timedelta
from typing import Any

from airflow.configuration import conf
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.temporal import TimeDeltaTrigger
from airflow.utils.context import Context

if not version.version.startswith("2.5"):
    import asyncio

    from airflow.triggers.base import BaseTrigger, TriggerEvent
    from airflow.utils import timezone

    class DateTimeTrigger(BaseTrigger):
        def __init__(self, moment):
            super().__init__()
            self.moment = moment

        def serialize(self):
            return (
                "airflow.triggers.temporal.DateTimeTrigger",
                {"moment": self.moment},
            )

        async def run(self):
            while self.moment > timezone.utcnow():
                await asyncio.sleep(1)
            yield TriggerEvent(self.moment)


class WaitFiveSecondsSensor(BaseSensorOperator):
    def __init__(
        self,
        deferrable: bool = conf.getboolean(
            "operators", "default_deferrable", fallback=False
        ),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.deferrable = deferrable

    def execute(self, context: Context) -> None:
        deferrable_supported = not version.version.startswith(
            "2.5"
        ) and not version.version.startswith("2.6")
        if self.deferrable or deferrable_supported:
            print("Deferring execution of the five second in the deferable mode ...")
            self.defer(
                trigger=TimeDeltaTrigger(timedelta(seconds=5)),
                method_name="execute_complete",
            )
        else:
            print("Sleeping for five second ...")
            time.sleep(5)

    def execute_complete(
        self,
        context: Context,
        event: dict[str, Any] | None = None,
    ) -> None:
        print("Execution of the five second sensor complete")
        return


def say_hello(**context):
    print(f"Hello from Airflow! At version: {version.version}")


def say_bye(**context):
    print(f"Bye from Airflow! At version: {version.version}")


with DAG(
    "triggerer", schedule_interval=None, start_date=datetime(2022, 1, 1), catchup=False
) as dag:

    say_hello = PythonOperator(task_id="say_hello", python_callable=say_hello)

    wait_five = WaitFiveSecondsSensor(task_id="triggerer")

    say_bye = PythonOperator(task_id="say_bye", python_callable=say_bye)

    say_hello >> wait_five >> say_bye

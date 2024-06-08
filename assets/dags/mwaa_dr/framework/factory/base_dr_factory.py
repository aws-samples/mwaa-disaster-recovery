"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import os
from abc import ABC, abstractmethod
from datetime import timedelta

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from mwaa_dr.framework.model.base_table import S3, BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel

BACKUP_RESTORE = "BACKUP_RESTORE"
WARM_STANDBY = "WARM_STANDBY"
EXECUTION_TIMEOUT = timedelta(minutes=5)


class BaseDRFactory(ABC):
    dag_id: str
    path_prefix: str
    storage_type: str
    batch_size: int
    model: DependencyModel[BaseTable]
    tables_cache: list[BaseTable]

    def __init__(
        self, dag_id: str, path_prefix: str, storage_type: str = None, batch_size=5000
    ) -> None:
        self.dag_id = dag_id
        self.path_prefix = path_prefix or "data"
        self.storage_type = storage_type or S3
        self.batch_size = batch_size
        self.tables_cache = None
        self.model = DependencyModel()

    @abstractmethod
    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        pass

    def tables(self) -> list[BaseTable]:
        if not self.tables_cache:
            self.tables_cache = self.setup_tables(self.model)
        return self.tables_cache

    def bucket(self, context=None) -> str:
        return BaseTable.bucket(context)

    def schedule(self) -> str:
        try:
            return Variable.get("DR_BACKUP_SCHEDULE")
        except Exception:
            return "@hourly"

    def task_token(self, context):
        dag_run = context.get("dag_run")
        return dag_run.conf["task_token"]

    def dag_run_result(self, context):
        dag_run = context.get("dag_run")
        task_instances = dag_run.get_task_instances()
        task_states = []
        for task in task_instances:
            task_states.append(f"{task.task_id} => {task.state}")
        return {"dag": dag_run.dag_id, "dag_run": dag_run.run_id, "tasks": task_states}

    def notify_success_to_sfn(self, **context):
        result = self.dag_run_result(context)
        result["status"] = "Success"

        if self.storage_type == S3:
            import json

            import boto3

            result["location"] = f"s3://{self.bucket(context)}/{self.path_prefix}"
            token = self.task_token(context)
            sfn = boto3.client("stepfunctions")
            sfn.send_task_success(taskToken=token, output=json.dumps(result))
        else:
            AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
            result["location"] = f"{AIRFLOW_HOME}/dags/{self.path_prefix}"

        print(result)

    def notify_failure_to_sfn(self, context):
        result = self.dag_run_result(context)
        result["status"] = "Fail"

        if self.storage_type == S3:
            import json

            import boto3

            token = self.task_token(context)
            sfn = boto3.client("stepfunctions")
            sfn.send_task_failure(
                taskToken=token, error="Restore Failure", cause=json.dumps(result)
            )

        print(result)

    def notify_failure_to_sns(self, context):
        result = self.dag_run_result(context)
        result["status"] = "Fail"

        if self.storage_type == S3:
            import json

            import boto3

            sns = boto3.client("sns")
            sns.publish(
                TopicArn=Variable.get("DR_SNS_TOPIC_ARN"),
                Subject=f"MWAA DR DAG Failure",
                Message=json.dumps(result, indent=2),
            )

        print(result)

    def dr_type(self, context=None) -> str:
        if context:
            dag_run = context.get("dag_run")
            if "dr_type" in dag_run.conf:
                return dag_run.conf["dr_type"]

        return WARM_STANDBY

    def setup_backup(self, **context):
        print("Executing the backup workflow setup ...")

        if self.storage_type == S3:
            print("No local file system setup necessary!")
            return

        AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
        path = os.path.join(AIRFLOW_HOME, "dags", self.path_prefix)

        if not os.path.exists(path):
            print(f"Creating directory path: {path}")
            os.mkdir(path)
        else:
            print(f"All folders in the path ({path}) exists! Skipping!")

    def teardown_backup(self, **context):
        print("Executing the backup workflow teardown ...")

    def setup_restore(self, **context):
        dr_type = self.dr_type(context)
        print(f"Executing the restore workflow setup for {dr_type} ...")

        if self.storage_type != S3:
            print("Checking folder path before local restore!")

            AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
            path = os.path.join(AIRFLOW_HOME, "dags", self.path_prefix)

            if not os.path.exists(path):
                AirflowFailException(f"The data directory at {path} does not exists!")
            else:
                print(f"All folders in the path ({path}) exists! Skipping!")

    def teardown_restore(self, **context):
        print("Executing the restore workflow teardown ...")

    def create_backup_dag(self) -> DAG:
        default_args = {
            "owner": "airflow",
            "start_date": days_ago(1),
            "execution_timeout": EXECUTION_TIMEOUT,
            "on_failure_callback": self.notify_failure_to_sns,
        }

        with DAG(
            dag_id=self.dag_id,
            schedule_interval=self.schedule(),
            catchup=False,
            default_args=default_args,
        ) as dag:
            setup_t = PythonOperator(
                task_id="setup", python_callable=self.setup_backup, provide_context=True
            )

            with TaskGroup(group_id="export_tables") as export_tables_t:
                for table in self.tables():
                    PythonOperator(
                        task_id=f"export_{table.name}s",
                        python_callable=table.backup,
                        provide_context=True,
                    )

            teardown_t = PythonOperator(
                task_id="teardown",
                python_callable=self.teardown_backup,
                provide_context=True,
            )

            setup_t >> export_tables_t >> teardown_t

        return dag

    def create_restore_dag(self) -> DAG:
        tables = self.tables()

        default_args = {
            "owner": "airflow",
            "start_date": days_ago(1),
            "execution_timeout": EXECUTION_TIMEOUT,
            "on_failure_callback": self.notify_failure_to_sfn,
        }

        with DAG(
            dag_id=self.dag_id,
            schedule_interval=None,
            catchup=False,
            default_args=default_args,
        ) as dag:
            setup_t = PythonOperator(
                task_id="setup",
                python_callable=self.setup_restore,
                provide_context=True,
            )

            restore_start_t = DummyOperator(task_id="restore_start")
            setup_t >> restore_start_t

            restore_tasks = dict()
            for table in tables:
                restore_tasks[table] = PythonOperator(
                    task_id=f"restore_{table.name}",
                    python_callable=table.restore,
                    provide_context=True,
                )

            restore_end_t = DummyOperator(task_id="restore_end")
            self.model.apply(restore_start_t, restore_tasks, restore_end_t)

            teardown_t = PythonOperator(
                task_id="teardown",
                python_callable=self.teardown_restore,
                provide_context=True,
            )
            restore_end_t >> teardown_t

            notify_success_t = PythonOperator(
                task_id="notify_success",
                python_callable=self.notify_success_to_sfn,
                provide_context=True,
            )
            teardown_t >> notify_success_t

        return dag

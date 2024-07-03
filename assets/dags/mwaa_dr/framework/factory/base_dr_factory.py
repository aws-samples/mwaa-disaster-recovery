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
from datetime import datetime

from airflow import DAG, settings
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup
from mwaa_dr.framework.model.base_table import S3, BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel

BACKUP_RESTORE = "BACKUP_RESTORE"
WARM_STANDBY = "WARM_STANDBY"


class BaseDRFactory(ABC):
    """
    Base class for creating Disaster Recovery (DR) DAGs.

    This abstract class provides a framework for creating DAGs that handle backup and restore operations
    for various data sources. It defines the common methods and properties required for both backup
    and restore DAGs.

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.

    Attributes:
        dag_id (str): The ID of the DAG.
        path_prefix (str): The prefix for the backup/restore path.
        storage_type (str): The type of storage used for backup/restore (e.g., S3, local file system).
        batch_size (int): The batch size for backup/restore operations.
        model (DependencyModel[BaseTable]): The dependency model for the tables.
        tables_cache (list[BaseTable]): A cache for the list of tables.
    """

    dag_id: str
    path_prefix: str
    storage_type: str
    batch_size: int
    model: DependencyModel[BaseTable]
    tables_cache: list[BaseTable]

    def __init__(
        self,
        dag_id: str,
        path_prefix: str = None,
        storage_type: str = None,
        batch_size=5000,
    ) -> None:
        self.dag_id = dag_id
        self.path_prefix = path_prefix or "data"
        self.storage_type = storage_type or S3
        self.batch_size = batch_size
        self.tables_cache = None
        self.model = DependencyModel()

    @abstractmethod
    def setup_tables(self, model: DependencyModel[BaseTable]) -> list[BaseTable]:
        """
        Abstract method to set up the list of tables for backup/restore operations.

        Args:
            model (DependencyModel[BaseTable]): The dependency model to be used when creating the tables and assigning dependencies.

        Returns:
            list[BaseTable]: The list of tables for backup/restore operations.
        """

    def tables(self) -> list[BaseTable]:
        """
        Get the list of tables for backup/restore operations.

        Returns:
            list[BaseTable]: The list of tables for backup/restore operations.
        """
        if not self.tables_cache:
            self.tables_cache = self.setup_tables(self.model)
        return self.tables_cache

    def bucket(self, context=None) -> str:
        """
        Get the S3 bucket name for backup/restore operations.

        Args:
            context (dict, optional): The Airflow task context.

        Returns:
            str: The S3 bucket name.
        """
        return BaseTable.bucket(context)

    def schedule(self) -> str:
        """
        Get the schedule for the backup DAG from the `DR_BACKUP_SCHEDULE` Airflow variable if available.

        Returns:
            str: The schedule for the backup DAG. Defaults to `None`.
        """
        return Variable.get("DR_BACKUP_SCHEDULE", default_var=None)

    def task_token(self, context, default_value=None):
        """
        Get the AWS StepFunctions task token from the Airflow dag_run context.

        Args:
            context (dict): The Airflow task context.
            default_value (str): The default value to return if the task token is not found.

        Returns:
            str: The task token.
        """
        try:
            dag_run = context.get("dag_run")
            return dag_run.conf["task_token"]
        except:
            return default_value

    def dag_run_result(self, context):
        """
        Get the DAG run result from the Airflow task context.

        Args:
            context (dict): The Airflow context.

        Returns:
            dict: The DAG run result.
        """
        dag_run = context.get("dag_run")
        task_instances = dag_run.get_task_instances()
        task_states = []
        for task in task_instances:
            task_states.append(f"{task.task_id} => {task.state}")
        return {"dag": dag_run.dag_id, "dag_run": dag_run.run_id, "tasks": task_states}

    def notify_success_to_sfn(self, **context):
        """
        Notify the success of the DAG run to AWS Step Functions.

        Args:
            **context: The Airflow context dictionary.
        """
        result = self.dag_run_result(context)
        result["status"] = "Success"

        if self.storage_type == S3:
            result["location"] = f"s3://{self.bucket(context)}/{self.path_prefix}"
            token = self.task_token(context, "--missing--")
            if token != "--missing--":
                import json
                import boto3

                sfn = boto3.client("stepfunctions")
                sfn.send_task_success(taskToken=token, output=json.dumps(result))
        else:
            AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
            result["location"] = f"{AIRFLOW_HOME}/dags/{self.path_prefix}"

        print(result)

    def notify_failure_to_sfn(self, context):
        """
        Notify the failure of the DAG run to AWS Step Functions.

        Args:
            context (dict): The Airflow context dictionary.
        """
        result = self.dag_run_result(context)
        result["status"] = "Fail"

        if self.storage_type == S3:
            import json

            import boto3

            token = self.task_token(context, "--missing--")
            if token != "--missing--":
                sfn = boto3.client("stepfunctions")
                sfn.send_task_failure(
                    taskToken=token, error="Restore Failure", cause=json.dumps(result)
                )

        print(result)

    def notify_failure_to_sns(self, context):
        """
        Notify the failure of the DAG run to AWS Simple Notification Service (SNS).

        Args:
            context (dict): The Airflow context dictionary.
        """
        result = self.dag_run_result(context)
        result["status"] = "Fail"

        if self.storage_type == S3:
            import json
            import boto3

            topic_arn = Variable.get("DR_SNS_TOPIC_ARN", "--missing--")

            if topic_arn != "--missing--":
                sns = boto3.client("sns")
                sns.publish(
                    TopicArn=topic_arn,
                    Subject="MWAA DR DAG Failure",
                    Message=json.dumps(result, indent=2),
                )

        print(result)

    def dr_type(self, context=None) -> str:
        """
        Get the disaster recovery type from the Airflow context dictionary.

        Args:
            context (dict, optional): The Airflow context dictionary.

        Returns:
            str: The disaster recovery type.
        """
        if context:
            dag_run = context.get("dag_run")
            if "dr_type" in dag_run.conf:
                return dag_run.conf["dr_type"]

        return WARM_STANDBY

    def setup_backup(self, **context):
        """
        A subclass hook/method to set up the backup workflow if necessary.

        Args:
            **context: The Airflow context dictionary.
        """
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
        """
        A subclass hook/method to tear down the backup workflow.

        Args:
            **context: The Airflow context dictionary.
        """
        print("Executing the backup workflow teardown ...")

    def setup_restore(self, **context):
        """
        A subclass hook/method to set up the restore workflow.

        Args:
            **context: The Airflow context dictionary.
        """
        dr_type = self.dr_type(context)
        print(f"Executing the restore workflow setup for {dr_type} ...")

        if self.storage_type != S3:
            print("Checking folder path before local restore!")

            AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
            path = os.path.join(AIRFLOW_HOME, "dags", self.path_prefix)

            if not os.path.exists(path):
                raise AirflowFailException(
                    f"The data directory at ({path}) does not exists!"
                )

            print(f"All folders in the path ({path}) exists for restore!")

    def teardown_restore(self, **context):
        """
        A subclass hook/method to tear down the restore workflow.

        Args:
            **context: The Airflow context dictionary.
        """
        print("Executing the restore workflow teardown ...")

    def create_backup_dag(self) -> DAG:
        """
        Create the backup DAG for the DR workflow.

        Returns:
            DAG: The backup DAG.
        """
        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
            "on_failure_callback": self.notify_failure_to_sns,
        }

        dag = DAG(
            dag_id=self.dag_id,
            schedule=self.schedule(),
            catchup=False,
            default_args=default_args,
        )

        setup_t = PythonOperator(
            task_id="setup", python_callable=self.setup_backup, dag=dag
        )

        with TaskGroup(group_id="export_tables", dag=dag) as export_tables_t:
            for table in self.tables():
                PythonOperator(
                    task_id=f"export_{table.name}s",
                    python_callable=table.backup,
                    dag=dag,
                )

        teardown_t = PythonOperator(
            task_id="teardown", python_callable=self.teardown_backup, dag=dag
        )

        setup_t >> export_tables_t >> teardown_t

        return dag

    def create_restore_dag(self) -> DAG:
        """
        Create the restore DAG for the DR workflow.

        Returns:
            DAG: The restore DAG.
        """
        tables = self.tables()

        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
            "on_failure_callback": self.notify_failure_to_sfn,
        }

        dag = DAG(
            dag_id=self.dag_id,
            schedule=None,
            catchup=False,
            default_args=default_args,
        )

        setup_t = PythonOperator(
            task_id="setup", python_callable=self.setup_restore, dag=dag
        )

        restore_start_t = DummyOperator(task_id="restore_start", dag=dag)
        setup_t >> restore_start_t

        restore_tasks = {}
        for table in tables:
            restore_tasks[table] = PythonOperator(
                task_id=f"restore_{table.name}", python_callable=table.restore, dag=dag
            )

        restore_end_t = DummyOperator(task_id="restore_end", dag=dag)
        self.model.apply(restore_start_t, restore_tasks, restore_end_t)

        teardown_t = PythonOperator(
            task_id="teardown", python_callable=self.teardown_restore, dag=dag
        )
        restore_end_t >> teardown_t

        notify_success_t = PythonOperator(
            task_id="notify_success",
            python_callable=self.notify_success_to_sfn,
            dag=dag,
        )
        teardown_t >> notify_success_t

        return dag

    def cleanup_tables(self, **context):
        """
        Cleans up the metadata store before a restore operation.

        Args:
            **context: The Airflow context dictionary.
        """
        try:
            from airflow.version import version
            from airflow.models import (
                DagModel,
                DagRun,
                DagTag,
                ImportError,
                Log,
                RenderedTaskInstanceFields,
                SlaMiss,
                TaskInstance,
                TaskFail,
                TaskReschedule,
                Trigger,
                XCom,
                Pool,
            )

            major_version, minor_version = int(version.split(".")[0]), int(
                version.split(".")[1]
            )
            if major_version >= 2 and minor_version >= 6:
                from airflow.jobs.job import Job
            else:
                from airflow.jobs.base_job import BaseJob as Job

            tables = [
                Job,
                TaskInstance,
                TaskFail,
                TaskReschedule,
                Trigger,
                DagTag,
                DagModel,
                DagRun,
                ImportError,
                Log,
                SlaMiss,
                RenderedTaskInstanceFields,
                XCom,
                Pool,
            ]

            print("Running metadata tables cleanup ...")

            with settings.Session() as session:
                for table in tables:
                    print(f"Deleting records from {table.__tablename__} ...")
                    query = session.query(table)
                    if table.__tablename__ == "job":
                        query = query.filter(Job.job_type != "SchedulerJob")
                    elif table.__tablename__ == "slot_pool":
                        query = query.filter(Pool.pool != "default_pool")
                    query.delete(synchronize_session=False)
                session.commit()

            print("Metadata cleanup complete!")
            self.notify_success_to_sfn(**context)
        except Exception as err:
            print(err)
            self.notify_failure_to_sfn(**context)
            raise err

    def create_cleanup_dag(self) -> DAG:
        """
        Creates the cleanup DAG to clean up metadata store before a restore operation.

        Returns:
            DAG: The cleanup DAG.
        """
        default_args = {
            "owner": "airflow",
            "start_date": datetime(2022, 1, 1),
        }
        dag = DAG(
            dag_id=self.dag_id,
            schedule=None,
            catchup=False,
            default_args=default_args,
        )
        PythonOperator(
            task_id="cleanup_tables", python_callable=self.cleanup_tables, dag=dag
        )
        return dag

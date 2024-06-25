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

from datetime import datetime

from airflow import DAG, settings
from airflow.operators.python import PythonOperator
from airflow.models import (
    DagModel,
    DagRun,
    DagTag,
    ImportError,
    Log,
    RenderedTaskInstanceFields,
    SlaMiss,
    TaskInstance,
    TaskReschedule,
    XCom,
)
from airflow.version import version

major_version, minor_version = int(version.split(".")[0]), int(version.split(".")[1])
if major_version >= 2 and minor_version >= 6:
    from airflow.jobs.job import Job
else:
    # The BaseJob class was renamed as of Apache Airflow v2.6
    from airflow.jobs.base_job import BaseJob as Job

TABLES_TO_CLEAN = [
    Job,
    TaskInstance,
    TaskReschedule,
    DagTag,
    DagModel,
    DagRun,
    ImportError,
    Log,
    SlaMiss,
    RenderedTaskInstanceFields,
    XCom,
]


def cleanup_tables():
    """
    Deletes all records from tables included in TABLES_TO_CLEAN
    """
    print("Running metadata tables cleanup ...")

    with settings.Session() as session:
        for table in TABLES_TO_CLEAN:
            print(f"Deleting records from {table.__tablename__} ...")
            query = session.query(table)
            if table.__tablename__ == "job":
                query = query.filter(Job.job_type != "SchedulerJob")
            query.delete(synchronize_session=False)
        session.commit()

    print("Metadata cleanup complete!")


default_args = {"owner": "airflow", "start_date": datetime(2022, 1, 1)}

with DAG(
    dag_id="cleanup_metadata",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:
    task = PythonOperator(task_id="cleanup_tables", python_callable=cleanup_tables)

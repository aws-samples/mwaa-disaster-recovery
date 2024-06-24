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

# Modified from https://docs.aws.amazon.com/mwaa/latest/userguide/samples-database-cleanup.html

from datetime import datetime
from time import sleep

from airflow import settings
from airflow.decorators import dag, task
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
from airflow.utils.dates import days_ago
from airflow.version import version

major_version, minor_version = int(version.split(".")[0]), int(version.split(".")[1])
if major_version >= 2 and minor_version >= 6:
    from airflow.jobs.job import Job
else:
    # The BaseJob class was renamed as of Apache Airflow v2.6
    from airflow.jobs.base_job import BaseJob as Job

TABLES_TO_CLEAN = {
    'job': Job,
    'task_instance': TaskInstance,
    'task_reschedule': TaskReschedule,
    'dag_tag', DagTag,
    'dag_model', DagModel,
    'dag_run': DagRun,
    'import_error': ImportError,
    'log': Log,
    'sla_miss': SlaMiss,
    'rendered_task_instance_fields': RenderedTaskInstanceFields,
    'xcom': XCom
}

@task()
def cleanup_db_fn(table):
    with settings.Session() as session:
        print(f"Deleting {table} ...")
        query = session.query(TABLES_TO_CLEAN[table])
        query.delete(synchronize_session=False)
        session.commit()
        print(f"Successfully deleted {table}!")


@dag(
    dag_id="cleanup_db",
    schedule=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=True,
)
def clean_db_dag_fn():
    t_last = None
    for x in TABLES_TO_CLEAN:
        t = cleanup_db_fn(x)
        if t_last:
            t_last >> t
        t_last = t


clean_db_dag = clean_db_dag_fn()

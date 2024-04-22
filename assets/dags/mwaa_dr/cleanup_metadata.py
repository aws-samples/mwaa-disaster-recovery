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

## Copied from https://docs.aws.amazon.com/mwaa/latest/userguide/samples-database-cleanup.html

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

# Delete entries for the past 35 days to 42 days. Adjust MAX_AGE_IN_DAYS and MIN_AGE_IN_DAYS
# to set how far back this DAG cleans the database.
# Note that this dag runs weekly.
MAX_AGE_IN_DAYS = 42
MIN_AGE_IN_DAYS = 35
DECREMENT = -7

# This is a list of (table, time) tuples.
# table = the table to clean in the metadata database
# time  = the column in the table associated to the timestamp of an entry
#         or None if not applicable.
TABLES_TO_CLEAN = [
    [Job, Job.latest_heartbeat],
    [TaskInstance, TaskInstance.execution_date],
    [TaskReschedule, TaskReschedule.execution_date],
    [DagTag, None],
    [DagModel, DagModel.last_parsed_time],
    [DagRun, DagRun.execution_date],
    [ImportError, ImportError.timestamp],
    [Log, Log.dttm],
    [SlaMiss, SlaMiss.execution_date],
    [RenderedTaskInstanceFields, RenderedTaskInstanceFields.execution_date],
    [XCom, XCom.execution_date],
]


@task()
def cleanup_db_fn(x):
    session = settings.Session()

    if x[1]:
        for oldest_days_ago in range(MAX_AGE_IN_DAYS, MIN_AGE_IN_DAYS, DECREMENT):
            earliest_days_ago = max(oldest_days_ago + DECREMENT, MIN_AGE_IN_DAYS)
            print(
                f"deleting {str(x[0])} entries between {earliest_days_ago} and {oldest_days_ago} days old..."
            )
            earliest_date = days_ago(earliest_days_ago)
            oldest_date = days_ago(oldest_days_ago)
            query = (
                session.query(x[0])
                .filter(x[1] >= oldest_date)
                .filter(x[1] <= earliest_date)
            )
            query.delete(synchronize_session=False)
            session.commit()
            sleep(5)
    else:
        # No time column specified for the table. Delete all entries
        print("deleting", str(x[0]), "...")
        query = session.query(x[0])
        query.delete(synchronize_session=False)
        session.commit()

    session.close()


@dag(
    dag_id="cleanup_db",
    schedule_interval="@weekly",
    start_date=days_ago(7),
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

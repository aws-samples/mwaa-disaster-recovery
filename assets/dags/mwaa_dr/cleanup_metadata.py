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
from airflow import DAG, settings
 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import (
    DAG, DagModel, DagRun, DagTag, ImportError, Log, SlaMiss, 
    RenderedTaskInstanceFields, TaskFail, TaskInstance, 
    TaskReschedule, XCom
)
from airflow import version
airflow_version = version.version

JobObject = None
if airflow_version.startswith('2.5'):
    from airflow.jobs.base_job import BaseJob
    JobObject = BaseJob
else:
    from airflow.jobs.job import Job
    JobObject = Job    

dag_id = "cleanup_metadata"

OBJECTS_TO_CLEAN = [
    DagModel, 
    DagRun, 
    DagTag,
    ImportError,
    Log, 
    SlaMiss,
    RenderedTaskInstanceFields,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    XCom
]
 
def cleanup_db(**kwargs):
    session = settings.Session()
    print("Session: ", str(session))

    for obj in OBJECTS_TO_CLEAN:
        query = session.query(obj)
        query.delete(synchronize_session=False)

    query = session.query(JobObject).filter(JobObject.job_type != 'SchedulerJob')
    query.delete(synchronize_session=False)
    session.commit()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id=dag_id, schedule_interval=None, catchup=False, default_args=default_args) as dag:                    
    task = PythonOperator(
        task_id="metadata_cleanup",
        python_callable=cleanup_db,
        provide_context=True     
    )

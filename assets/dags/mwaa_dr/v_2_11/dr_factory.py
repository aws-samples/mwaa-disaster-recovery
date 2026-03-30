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

from mwaa_dr.v_2_10.dr_factory import DRFactory_2_10


class DRFactory_2_11(DRFactory_2_10):
    """
    Factory class for creating database models for Apache Airflow 2.11.x.

    This class inherits from DRFactory_2_10 as there are no database schema
    changes between Airflow 2.10.3 and 2.11.x. The database migrations remain
    the same (up to migration 0152).

    For reference:
    - Airflow 2.11.0 Release: https://airflow.apache.org/docs/apache-airflow/2.11.0/release_notes.html
    - Database ERD: https://airflow.apache.org/docs/apache-airflow/2.11.0/database-erd-ref.html

    Args:
        dag_id (str): The ID of the DAG.
        path_prefix (str, optional): The prefix for the backup/restore path. Defaults to "data".
        storage_type (str, optional): The type of storage used for backup/restore. Defaults to S3.
        batch_size (int, optional): The batch size for backup/restore operations. Defaults to 5000.
    """

    pass  # No schema changes from 2.10.x - inherits all table definitions

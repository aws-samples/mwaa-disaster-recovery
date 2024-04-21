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

from airflow import settings

from mwaa_dr.framework.model.base_table  import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from io import StringIO

class ActiveDagTable(BaseTable):
    def __init__(
            self, 
            model: DependencyModel,
            storage_type: str = None,
            path_prefix: str = None,
            batch_size = 5000
    ):
        super().__init__(
            name='active_dag',
            model=model,
            storage_type=storage_type,
            path_prefix=path_prefix,
            batch_size = batch_size
        )


    def backup(self, **context):
        session = settings.Session()
        session.execute("DROP TABLE IF EXISTS active_dags")
        session.execute("CREATE TABLE active_dags AS SELECT dag_id FROM dag WHERE NOT is_paused AND is_active AND dag_id NOT IN ('backup_metadata', 'restore_metadata')")
        session.commit()

        rows = session.execute("SELECT * FROM active_dags").all()
        buffer = StringIO('')
        if len(rows) > 0:
            for row in rows:
                buffer.write(f'{row.dag_id}\n')
        
        self.write(buffer.getvalue(), context)
        session.close()


    def restore(self, **context):
        backup_file = self.read(context)

        session = settings.Session()
        conn = settings.engine.raw_connection()

        try:
            session.execute(f"CREATE TABLE IF NOT EXISTS active_dags(dag_id VARCHAR(250))")
            session.commit()
            with open(backup_file, 'r') as backup:
                cursor = conn.cursor()
                cursor.copy_expert("COPY active_dags FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", backup)
                conn.commit()
            session.execute(f"UPDATE dag d SET is_paused=false FROM active_dags ad WHERE d.dag_id = ad.dag_id")
            session.commit()
        finally:
            conn.close()

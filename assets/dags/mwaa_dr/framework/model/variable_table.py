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

import csv
from io import StringIO

from airflow import settings
from airflow.models import Variable
from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel


class VariableTable(BaseTable):
    def __init__(
        self,
        model: DependencyModel,
        storage_type: str = None,
        path_prefix: str = None,
        batch_size=5000,
    ):
        super().__init__(
            name="variable",
            model=model,
            storage_type=storage_type,
            path_prefix=path_prefix,
            batch_size=batch_size,
        )

    def backup(self, **context):
        session = settings.Session()

        query = session.query(Variable)
        rows = query.all()

        buffer = StringIO("")
        keys = ["key", "val", "description"]
        if len(rows) > 0:
            writer = csv.DictWriter(buffer, keys)
            for row in rows:
                writer.writerow(
                    {keys[0]: row.key, keys[1]: row.get_val(), keys[2]: row.description}
                )

        self.write(buffer.getvalue(), context)
        session.close()

    def restore(self, **context):
        missing = "--missing--"
        backup_file = self.read(context)
        session = settings.Session()

        with open(backup_file) as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                var = Variable.get(key=row[0], default_var=missing)
                if var == missing:
                    Variable.set(
                        key=row[0], value=row[1], description=row[2], session=session
                    )

        session.commit()
        session.close()

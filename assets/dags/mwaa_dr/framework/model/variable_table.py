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
    """
    A class representing the Airflow Variable table. Inherits from the BaseTable class.

    This class provides methods to backup and restore Airflow Variables to/from a storage location.
    It is part of the mwaa-disaster-recovery framework for managing Airflow metadata backups.

    Args:
        model (DependencyModel): The DependencyModel instance representing table dependencies.
        storage_type (str, optional): The storage type to use (e.g. 'S3', 'LOCAL_FS'). Defaults to S3.
        path_prefix (str, optional): The path prefix for storage location. Defaults to None.
        batch_size (int, optional): The batch size for writing to storage. Defaults to 5000.
    """

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
        """
        Backup the Airflow Variables to the configured storage location.

        Args:
            **context: Additional context parameters passed from the Airflow task.

        Returns:
            None
        """
        with settings.Session() as session:
            query = session.query(Variable)
            rows = query.all()

            buffer = StringIO("")
            keys = ["key", "val", "description"]
            if rows:
                writer = csv.DictWriter(buffer, keys)
                for row in rows:
                    writer.writerow(
                        {
                            keys[0]: row.key,
                            keys[1]: row.get_val(),
                            keys[2]: row.description,
                        }
                    )

            self.write(buffer.getvalue(), context)

    def restore(self, **context):
        """
        Restore the Airflow Variables from the configured storage location.
        Only variables that do not already exist will be restored from backup.

        Args:
            **context: Additional context parameters passed from the Airflow task.

        Returns:
            None
        """
        missing = "--missing--"
        csv_file = self.read(context)

        try:
            with settings.Session() as session:
                reader = csv.reader(csv_file)
                for row in reader:
                    var = Variable.get(key=row[0], default_var=missing)
                    if var == missing:
                        Variable.set(
                            key=row[0],
                            value=row[1],
                            description=row[2],
                            session=session,
                        )

                session.commit()
        finally:
            csv_file.close()

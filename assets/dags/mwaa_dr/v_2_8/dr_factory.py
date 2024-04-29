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

from mwaa_dr.framework.model.base_table import BaseTable
from mwaa_dr.framework.model.dependency_model import DependencyModel
from mwaa_dr.v_2_7.dr_factory import DRFactory_2_7

"""
The schema and dependencies are based on ERD here:
https://airflow.apache.org/docs/apache-airflow/2.8.1/database-erd-ref.html
"""


class DRFactory_2_8(DRFactory_2_7):
    def __init__(
        self, dag_id: str, path_prefix: str, storage_type: str = None, batch_size=5000
    ) -> None:
        super().__init__(dag_id, path_prefix, storage_type, batch_size)

    def dag_run(self, model: DependencyModel[BaseTable]) -> BaseTable:
        return BaseTable(
            name="dag_run",
            model=model,
            columns=[
                "clear_number",  # New Field
                "conf",
                "creating_job_id",
                "dag_hash",
                "dag_id",
                "data_interval_end",
                "data_interval_start",
                "end_date",
                "execution_date",
                "external_trigger",
                "last_scheduling_decision",
                "log_template_id",
                "queued_at",
                "run_id",
                "run_type",
                "start_date",
                "state",
                "updated_at",
            ],
            export_mappings=dict(conf="'\\x' || encode(conf,'hex') as conf"),
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def log(self, model: DependencyModel[BaseTable]) -> BaseTable:
        return BaseTable(
            name="log",
            model=model,
            columns=[
                "dag_id",
                "dttm",
                "event",
                "execution_date",
                "extra",
                "map_index",
                "owner",
                "owner_display_name",  # New Field
                "task_id",
            ],
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

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
from mwaa_dr.v_2_6.dr_factory import DRFactory_2_6

"""
The schema and dependencies are based on ERD here:
https://airflow.apache.org/docs/apache-airflow/2.7.2/database-erd-ref.html
"""


class DRFactory_2_7(DRFactory_2_6):
    def __init__(
        self, dag_id: str, path_prefix: str, storage_type: str = None, batch_size=5000
    ) -> None:
        super().__init__(dag_id, path_prefix, storage_type, batch_size)

    def task_instance(self, model: DependencyModel[BaseTable]) -> BaseTable:
        return BaseTable(
            name="task_instance",
            model=model,
            columns=[
                "dag_id",
                "map_index",
                "run_id",
                "task_id",
                "custom_operator_name",  # New field
                "duration",
                "end_date",
                "executor_config",
                "external_executor_id",
                "hostname",
                "job_id",
                "max_tries",
                "next_kwargs",
                "next_method",
                "operator",
                "pid",
                "pool",
                "pool_slots",
                "priority_weight",
                "queue",
                "queued_by_job_id",
                "queued_dttm",
                "start_date",
                "state",
                "trigger_id",
                "trigger_timeout",
                "try_number",
                "unixname",
                "updated_at",
            ],
            export_mappings=dict(
                executor_config="'\\x' || encode(executor_config,'hex') as executor_config"
            ),
            export_filter="state NOT IN ('running','restarting','queued','scheduled', 'up_for_retry','up_for_reschedule')",
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

    def slot_pool(self, model: DependencyModel[BaseTable]) -> BaseTable:
        return BaseTable(
            name="slot_pool",
            model=model,
            columns=[
                "description",
                "include_deferred",  # New field
                "pool",
                "slots",
            ],
            export_filter="pool != 'default_pool'",
            storage_type=self.storage_type,
            path_prefix=self.path_prefix,
            batch_size=self.batch_size,
        )

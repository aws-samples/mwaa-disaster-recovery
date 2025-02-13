# pylint: disable=missing-class-docstring
# pylint: disable=missing-function-docstring

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

from sure import expect

from tests.unit.mwaa_dr.v_2_4.test_dr_factory_2_4 import check_base_table
from mwaa_dr.v_2_9.dr_factory import DRFactory_2_9


class TestDRFactory_2_9:
    def test_log(self):
        factory = DRFactory_2_9("log")

        check_base_table(
            factory=factory,
            actual_table=factory.log(factory.model),
            expected_name="log",
            expected_columns=[
                "dag_id",
                "dttm",
                "event",
                "execution_date",
                "extra",
                "map_index",
                "owner",
                "owner_display_name",
                "run_id",  # New Field
                "task_id",
            ],
        )

    def test_task_instance(self):
        factory = DRFactory_2_9("dag")

        check_base_table(
            factory=factory,
            actual_table=factory.task_instance(factory.model),
            expected_name="task_instance",
            expected_columns=[
                "dag_id",
                "map_index",
                "run_id",
                "task_id",
                "custom_operator_name", # New Field
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
                "rendered_map_index",  # New Field
                "start_date",
                "state",
                "task_display_name",  # New Field
                "trigger_id",
                "trigger_timeout",
                "try_number",
                "unixname",
                "updated_at", # New Field
            ],
            expected_mappings={ "executor_config": "'\\x' || encode(executor_config,'hex') as executor_config" },
            expected_export_filter="state NOT IN ('running','restarting','queued','scheduled', 'up_for_retry','up_for_reschedule')"
        )

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
from mwaa_dr.v_2_8.dr_factory import DRFactory_2_8


class TestDRFactory_2_8:
    def test_dag_run(self):
        factory = DRFactory_2_8("dag")

        check_base_table(
            factory=factory,
            actual_table=factory.dag_run(factory.model),
            expected_name="dag_run",
            expected_columns=[
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
            expected_mappings={"conf": "'\\x' || encode(conf,'hex') as conf"},
        )

    def test_log(self):
        factory = DRFactory_2_8("log")

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
                "owner_display_name",  # New Field
                "task_id",
            ],
        )

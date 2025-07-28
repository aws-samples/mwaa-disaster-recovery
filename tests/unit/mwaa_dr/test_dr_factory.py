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
from unittest.mock import patch
from sure import expect
from mwaa_dr.framework.factory.default_dag_factory import DefaultDagFactory
from mwaa_dr.v_2_4.dr_factory import DRFactory_2_4
from mwaa_dr.v_2_5.dr_factory import DRFactory_2_5
from mwaa_dr.v_2_6.dr_factory import DRFactory_2_6
from mwaa_dr.v_2_7.dr_factory import DRFactory_2_7
from mwaa_dr.v_2_8.dr_factory import DRFactory_2_8
from mwaa_dr.v_2_9.dr_factory import DRFactory_2_9
from mwaa_dr.v_2_10.dr_factory import DRFactory_2_10
from mwaa_dr import dr_factory
import importlib
import pytest


class TestDRFactory:
    @pytest.mark.parametrize(
        "airflow_version,factory_module",
        [
            ("-1", DefaultDagFactory),
            ("2.4.0", DRFactory_2_4),
            ("2.5.0", DRFactory_2_5),
            ("2.6.0", DRFactory_2_6),
            ("2.7.0", DRFactory_2_7),
            ("2.8.0", DRFactory_2_8),
            ("2.9.0", DRFactory_2_9),
            ("2.10.0", DRFactory_2_10),
        ],
    )
    def test_factory_matches_version(self, airflow_version, factory_module):
        with patch("airflow.version.version", airflow_version):
            importlib.reload(dr_factory)

            expect(dr_factory.DRFactory).to.equal(factory_module)

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

from airflow import version


airflow_version = version.version


if airflow_version.startswith("2.4"):
    from mwaa_dr.v_2_4.dr_factory import DRFactory_2_4

    DRFactory = DRFactory_2_4

elif airflow_version.startswith("2.5"):
    from mwaa_dr.v_2_5.dr_factory import DRFactory_2_5

    DRFactory = DRFactory_2_5

elif airflow_version.startswith("2.6"):
    from mwaa_dr.v_2_6.dr_factory import DRFactory_2_6

    DRFactory = DRFactory_2_6

elif airflow_version.startswith("2.7"):
    from mwaa_dr.v_2_7.dr_factory import DRFactory_2_7

    DRFactory = DRFactory_2_7

elif airflow_version.startswith("2.8"):
    from mwaa_dr.v_2_8.dr_factory import DRFactory_2_8

    DRFactory = DRFactory_2_8

elif airflow_version.startswith("2.9"):
    from mwaa_dr.v_2_9.dr_factory import DRFactory_2_9

    DRFactory = DRFactory_2_9

elif airflow_version.startswith("2.10"):
    from mwaa_dr.v_2_10.dr_factory import DRFactory_2_10

    DRFactory = DRFactory_2_10

else:
    from mwaa_dr.framework.factory.default_dag_factory import DefaultDagFactory

    DRFactory = DefaultDagFactory

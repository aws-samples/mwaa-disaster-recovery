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

import subprocess
import pytest

V_2_5 = '2_5'
V_2_6 = '2_6'
V_2_7 = '2_7'
V_2_8 = '2_8'

def setup(version: str):
    result = subprocess.run(
        ['./build.sh', 'setup', version],        
        capture_output=True,
        text=True,
        shell=True
    )
    output = result.stdout.strip()
    print(output)

def teardown(version: str):
    result = subprocess.run(
        ['./build.sh', 'teardown', version],        
        capture_output=True,
        text=True,
        shell=True
    )
    output = result.stdout.strip()
    print(output)

def trigger_dag(dag_name: str):
    print(f'Triggering {dag_name} ...')
    result = subprocess.run(
        ['./build.sh', 'trigger', dag_name],
        capture_output=True,
        text=True,
        shell=True
    )
    output = result.stdout.strip()
    print(output)

@pytest.fixture
def mwaa_local_runner(request):
    setup(request.param)
    yield request.param
    # teardown(request.param)

@pytest.mark.parametrize('mwaa_local_runner', [V_2_8], indirect=True)
def test_import_export(mwaa_local_runner):
    print(f'{mwaa_local_runner} test dummy done!')

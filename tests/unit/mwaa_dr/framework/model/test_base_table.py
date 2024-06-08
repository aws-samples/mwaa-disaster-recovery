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

from assets.dags.mwaa_dr.framework.model.base_table import BaseTable
from assets.dags.mwaa_dr.framework.model.dependency_model import DependencyModel

from sure import expect

class TestBaseTable:
    def test_construction_defaults(self):
        model = DependencyModel()
        table = BaseTable(
            name="test_table",
            model=model,
        )
    
        expect(table.name).to.equal("test_table")
        expect(table.model).to.equal(model)
        expect(table.columns).to.equal([])
        expect(table.export_mappings).to.equal({})
        expect(table.export_filter).to.equal('')
        expect(table.storage_type).to.equal('S3')
        expect(table.path_prefix).to.be.falsy
        expect(table.batch_size).to.equal(5000)


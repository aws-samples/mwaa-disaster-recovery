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

from unittest.mock import patch, mock_open

from airflow.models import Connection
from sure import expect

from mwaa_dr.framework.model.connection_table import ConnectionTable
from mwaa_dr.framework.model.dependency_model import DependencyModel

class TestConnectionTable:
    def test_construction(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        expect(table.model).to.equal(model)
        expect(table.storage_type).to.equal('LOCAL_FS')
        expect(table.path_prefix).to.equal('data')
        expect(table.batch_size).to.equal(1000)
        expect(table.columns).to.equal([
            "conn_id",
            "conn_type",
            "description",
            "extra",
            "host",
            "login",
            "password",
            "port",
            "schema",
        ])

    def test_backup_with_connections(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        context = dict()

        with (
            patch('sqlalchemy.orm.Session.query') as query,
            patch.object(table, 'write') as write
        ):
            query.return_value.all.return_value = [
                Connection(
                    conn_id='id',
                    conn_type='type',
                    description='description',
                    extra='{"extra": "value"}',
                    host='host',
                    login='login',
                    password='password',
                    port=1234,
                    schema='schema'
                )
            ]

            table.backup(**context)

            write.assert_called_once_with(
                'id,type,description,"{""extra"": ""value""}",host,login,password,1234,schema\r\n',
                context
            )

    def test_backup_without_connections(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        context = dict()

        with (
            patch('sqlalchemy.orm.Session.query') as query,
            patch.object(table, 'write') as write
        ):
            query.return_value.all.return_value = []

            table.backup(**context)

            write.assert_called_once_with('', context)
    
    def test_restore_non_existing_connection(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        context = dict()
        data = 'id,type,description,"{""extra"": ""value""}",host,login,password,1234,schema\r\n'

        with (
            patch('builtins.open', mock_open(read_data=data)) as mock_file,
            patch.object(table, 'read', return_value='connection.csv') as read,
            patch('sqlalchemy.orm.Session.__enter__') as session
        ):
            session.return_value.query.return_value.filter.return_value.all.return_value = []

            table.restore(**context)

            # Cannot do object comparison for the session.add_all() call because Connection does on implement __eq__
            session.return_value.add_all.assert_called_once()
            session.return_value.add_all.assert_called_once()

    def test_restore_existing_connection(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        context = dict()
        data = 'id,type,description,"{""extra"": ""value""}",host,login,password,1234,schema\r\n'

        with (
            patch('builtins.open', mock_open(read_data=data)) as mock_file,
            patch.object(table, 'read', return_value='connection.csv') as read,
            patch('sqlalchemy.orm.Session.__enter__') as session
        ):
            session.return_value.query.return_value.filter.return_value.all.return_value = [
                Connection(
                    conn_id='id',
                    conn_type='type',
                    description='description',
                    extra='{"extra": "value"}',
                    host='host',
                    login='login',
                    password='XXXXXXXX',
                    port=1234,
                    schema='schema'
                )
            ]

            table.restore(**context)

            session.return_value.add_all.assert_not_called()
            session.return_value.add_all.assert_not_called()

    def test_restore_missing_port(self):
        model = DependencyModel()
        table = ConnectionTable(
            model=model,
            storage_type='LOCAL_FS',
            path_prefix='data',
            batch_size=1000
        )
        context = dict()
        data = 'id,type,description,"{""extra"": ""value""}",host,login,password,,schema\r\n'

        with (
            patch('builtins.open', mock_open(read_data=data)) as mock_file,
            patch.object(table, 'read', return_value='connection.csv') as read,
            patch('sqlalchemy.orm.Session.__enter__') as session
        ):
            session.return_value.query.return_value.filter.return_value.all.return_value = []

            table.restore(**context)

            # Cannot do object comparison for the session.add_all() call because Connection does on implement __eq__
            session.return_value.add_all.assert_called_once()
            session.return_value.add_all.assert_called_once()

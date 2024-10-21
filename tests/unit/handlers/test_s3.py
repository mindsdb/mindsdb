from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from botocore.client import ClientError
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import Select, Identifier, Star, Constant

import pandas as pd

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.handlers.s3_handler.s3_handler import S3Handler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)


class TestS3Handler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def object_name(self):
        return '`my-bucket/my-file.csv`'

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            aws_access_key_id='AQAXEQK89OX07YS34OP',
            aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            bucket='mindsdb-bucket',
            region_name='us-east-2',
        )

    def create_handler(self):
        return S3Handler('s3', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('boto3.client')

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that duckdb.connect is called exactly once.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    @patch('boto3.client')
    def test_check_connection_success(self, mock_boto3_client):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    @patch('boto3.client')
    def test_check_connection_failure_invalid_bucket_or_no_access(self, mock_boto3_client):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on failed connection due to invalid bucket or lack of access permissions.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.head_bucket.side_effect = ClientError(
            error_response={
                'Error': {
                    'Code': '404',
                    'Message': 'Not Found',
                }
            },
            operation_name='HeadBucket'
        )

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    @patch('boto3.client')
    def test_query_select(self, mock_boto3_client):
        """
        Tests the `query` method to ensure it executes a SELECT SQL query using a mock cursor and returns a Response object.
        SELECT works somewhat differently than the other queries, so it is tested separately.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance

        duckdb_connect = MagicMock()
        self.handler._connect_duckdb = duckdb_connect
        duckdb_execute = duckdb_connect().__enter__().execute
        duckdb_execute().fetchdf.return_value = pd.DataFrame([], columns=['col_2'])

        # Craft the SELECT query and execute it.
        object_name = 'my-bucket/my-file.csv'
        select = ast.Select(
            targets=[
                Star()
            ],
            from_table=Identifier(
                parts=[object_name]
            )
        )

        duckdb_execute.reset_mock()
        response = self.handler.query(select)

        duckdb_execute.assert_called_once_with(
            f"SELECT * FROM 's3://{self.dummy_connection_data['bucket']}/{object_name.replace('`', '')}'"
        )

        assert isinstance(response, Response)
        self.assertFalse(response.error_code)

    @patch('boto3.client')
    def test_query_insert(self, mock_boto3_client):
        """
        Tests the `query` method to ensure it executes a INSERT SQL query using a mock cursor and returns a Response object.
        INSERT works similarly to UPDATE and DELETE.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.head_object.return_value = MagicMock()

        duckdb_connect = MagicMock()
        self.handler._connect_duckdb = duckdb_connect
        duckdb_execute = duckdb_connect().__enter__().execute
        duckdb_execute().fetchdf.return_value = None

        # Craft the INSERT query and execute it.
        columns = ['col_1', 'col_2']
        values = [('val_1', 'val_2')]
        insert = ast.Insert(
            table=Identifier(
                parts=[self.object_name]
            ),
            columns=columns,
            values=values
        )
        duckdb_execute.reset_mock()
        response = self.handler.query(insert)

        sqls = [i[0][0] for i in duckdb_execute.call_args_list]
        assert sqls[0] == f"CREATE TABLE tmp_table AS SELECT * FROM 's3://{self.dummy_connection_data['bucket']}/{self.object_name}'"

        assert sqls[1] == "INSERT INTO tmp_table BY NAME SELECT * FROM df"

        assert sqls[2] == f"COPY tmp_table TO 's3://{self.dummy_connection_data['bucket']}/{self.object_name}'"

        assert isinstance(response, Response)
        self.assertFalse(response.error_code)

    @patch('boto3.client')
    def test_get_tables(self, mock_boto3_client):
        """
        Test that the `get_tables` method correctly calls the `list_objects_v2` method and returns a Response object with the supported objects (files).
        """
        # Mock the boto3 client object and its methods.
        mock_boto3_client_instance = MagicMock()
        mock_boto3_client.return_value = mock_boto3_client_instance
        mock_boto3_client_instance.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'file1.csv'},
                {'Key': 'file2.tsv'},
                {'Key': 'file3.json'},
                {'Key': 'file4.parquet'},
                {'Key': 'file5.xlsx'},
            ]
        }

        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 5)  # +1 table is 'files'
        self.assertNotIn('file5.xlsx', df['table_name'].values)

    @patch('mindsdb.integrations.handlers.s3_handler.s3_handler.S3Handler.query')
    def test_get_columns(self, mock_query):
        """
        Test that the `get_columns` method correctly constructs the SQL query and calls `native_query` with the correct query.
        """
        mock_query.return_value = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                data={
                    'col_1': ['row_1', 'row_2', 'row_3'],
                    'col_2': [1, 2, 3],
                },
            )
        )

        table_name = 'mock_table'
        response = self.handler.get_columns(table_name)

        expected_query = Select(
            targets=[Star()],
            from_table=Identifier(parts=[table_name]),
            limit=Constant(1)
        )
        self.handler.query.assert_called_once_with(expected_query)

        df = response.data_frame
        self.assertEqual(df.columns.tolist(), ['column_name', 'data_type'])
        self.assertEqual(df['data_type'].values.tolist(), ['string', 'int64'])


if __name__ == '__main__':
    unittest.main()

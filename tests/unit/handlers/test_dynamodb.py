import unittest
from collections import OrderedDict
from botocore.client import ClientError
from unittest.mock import patch, MagicMock, Mock

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
from mindsdb_sql.parser.ast.select.identifier import Identifier

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.dynamodb_handler.dynamodb_handler import DynamoDBHandler


class TestDynamoDBHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            aws_access_key_id='AQAXEQK89OX07YS34OP',
            aws_secret_access_key='wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            region_name='us-east-2',
        )

    def create_handler(self):
        return DynamoDBHandler('dynamodb', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('boto3.client')

    def test_connect_failure_with_missing_connection_data(self):
        """
        Test if `connect` method raises ValueError when required connection parameters are missing.
        """
        self.handler.connection_data = {}
        with self.assertRaises(ValueError):
            self.handler.connect()

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that boto3.client is called exactly once.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_check_connection_failure_with_incorrect_credentials(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on failed connection due to incorrect credentials.
        """
        self.mock_connect.return_value.list_tables.side_effect = ClientError(
            error_response={'Error': {'Code': 'AccessDeniedException', 'Message': 'Access Denied'}},
            operation_name='list_tables'
        )

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_check_connection_success(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        self.mock_connect.return_value.list_tables.return_value = {'TableNames': ['table1', 'table2']}
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_query_select_success(self):
        """
        Test if the `query` method returns a response object with a data frame containing the query result.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        mock_boto3_client = Mock()
        mock_boto3_client.execute_statement.return_value = {
            'Items': [
                {'id': {'N': '1'}, 'name': {'S': 'Alice'}},
                {'id': {'N': '2'}, 'name': {'S': 'Bob'}}
            ]
        }

        self.handler.connect = MagicMock(return_value=mock_boto3_client)
        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier('table1')
        )
        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['id', 'name'])
        self.assertEqual(df['id'].tolist(), [1, 2])
        self.assertEqual(df['name'].tolist(), ['Alice', 'Bob'])

    def test_query_select_failure_with_unsupported_clause(self):
        """
        Test if the `query` method raises ValueError on a SELECT query with an unsupported clause.
        """
        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier('table1'),
            limit=10
        )
        with self.assertRaises(ValueError):
            self.handler.query(query)

    def test_query_insert_failure(self):
        """
        Test if the `query` method raises ValueError on an INSERT query. INSERT queries are not supported by this handler at the moment.
        """
        mock_boto3_client = Mock()
        mock_boto3_client.execute_statement.return_value = {}

        self.handler.connect = MagicMock(return_value=mock_boto3_client)
        query = ast.Insert(
            table=Identifier('table1'),
            columns=['id', 'name'],
            values=[[1, 'Alice']]
        )
        with self.assertRaises(ValueError):
            self.handler.query(query)

    def test_get_tables(self):
        """
        Test if the `get_tables` method returns a response object with a list of tables.
        """
        mock_boto3_client = Mock()
        mock_boto3_client.list_tables.return_value = {'TableNames': ['table1', 'table2']}

        self.handler.connection = mock_boto3_client
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['table_name'])
        self.assertEqual(df['table_name'].tolist(), ['table1', 'table2'])

    def test_get_columns(self):
        """
        Test if the `get_columns` method returns a response object with a list of columns for a given table.
        """
        mock_boto3_client = Mock()
        mock_boto3_client.describe_table.return_value = {
            'Table': {
                'KeySchema': [
                    {'AttributeName': 'id', 'KeyType': 'HASH'},
                    {'AttributeName': 'name', 'KeyType': 'RANGE'}
                ],
                'AttributeDefinitions': [
                    {'AttributeName': 'id', 'AttributeType': 'N'},
                    {'AttributeName': 'name', 'AttributeType': 'S'}
                ]
            }
        }

        self.handler.connection = mock_boto3_client
        response = self.handler.get_columns('table1')

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 2)
        self.assertEqual(df.columns.tolist(), ['column_name', 'data_type'])
        self.assertEqual(df['column_name'].tolist(), ['id', 'name'])
        self.assertEqual(df['data_type'].tolist(), ['N', 'S'])


if __name__ == '__main__':
    unittest.main()

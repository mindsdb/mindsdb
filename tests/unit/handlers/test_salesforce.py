from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

from salesforce_api.exceptions import AuthenticationError
from mindsdb_sql_parser.ast import BinaryOperation, Constant, Identifier, Select, Star

from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.salesforce_handler.salesforce_handler import SalesforceHandler
from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import create_table_class
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)


class TestSalesforceHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username='demo@example.com',
            password='demo_password',
            client_id='3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY',
            client_secret='5A52C1A1E21DF9012IODC9ISNXXAADDA9',
        )

    def create_handler(self):
        return SalesforceHandler('salesforce', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('salesforce_api.Salesforce')

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that salesforce_api.Salesforce is instantiated exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_check_connection_success(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a failed connection.
        """
        self.mock_connect.side_effect = AuthenticationError('Invalid credentials')
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_get_tables(self):
        """
        Test that the `get_tables` method returns a list of tables mapped from the Salesforce API.
        """
        mock_tables = ['Account', 'Contact']
        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                describe=lambda: {'sobjects': [{'name': table} for table in mock_tables]}
            )
        )
        self.handler.connect()
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_tables))
        self.assertEqual(list(df['table_name']), [table.lower() for table in mock_tables])

    def test_get_columns(self):
        """
        Test that the `get_columns` method returns a list of columns for a given table.
        """
        mock_columns = ['Id', 'Name', 'Email']
        mock_table = 'Contact'
        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                describe=lambda: {'sobjects': [{'name': mock_table}]},
                Contact=MagicMock(
                    describe=lambda: {'fields': [{'name': column} for column in mock_columns]}
                )
            )
        )
        self.handler.connect()
        response = self.handler.get_columns(mock_table.lower())

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_columns))
        self.assertEqual(list(df['Field']), mock_columns)


class TestSalesforceAnyTable(BaseAPIResourceTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username='demo@example.com',
            password='demo_password',
            client_id='3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY',
            client_secret='5A52C1A1E21DF9012IODC9ISNXXAADDA9',
        )

    def create_handler(self):
        return SalesforceHandler('salesforce', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('salesforce_api.Salesforce')

    def create_resource(self):
        return create_table_class(self.table_name)(self.handler)

    def setUp(self):
        """
        Set up common test fixtures.
        """
        self.table_name = 'Contact'
        self.mock_columns = ['Id', 'Name', 'Email']
        self.mock_record = {column: f'{column}_value' for column in self.mock_columns}

        super().setUp()

        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                query=lambda query: [
                    {
                        'attributes': {'type': self.table_name},
                        **self.mock_record
                    }
                ]
            ),
            Contact=MagicMock(
                describe=lambda: {'fields': [{'name': column} for column in self.mock_columns]}
            )
        )

    def test_select_all(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a simple SELECT * query.
        """
        select_query = Select(
            targets=[
                Star()
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            )
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns.
        """
        select_query = Select(
            targets=[
                Identifier(
                    parts=[
                        column
                    ]
                ) for column in self.mock_columns
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            )
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_alias(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and aliases.
        """
        select_query = Select(
            targets=[
                Identifier(
                    parts=[
                        column
                    ],
                    alias=Identifier(
                        parts=[
                            f'{column}_alias'
                        ]
                    )
                ) for column in self.mock_columns
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            )
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), [f'{column}_alias' for column in self.mock_columns])
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_condition(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and a WHERE condition.
        """
        select_query = Select(
            targets=[
                Identifier(
                    parts=[
                        column
                    ]
                ) for column in self.mock_columns
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            ),
            where=BinaryOperation(
                op='=',
                args=[
                    Identifier('Id'),
                    Constant('Id_value')
                ]
            )
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_condition_and_limit(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns, a WHERE condition, and a LIMIT clause.
        """
        select_query = Select(
            targets=[
                Identifier(
                    parts=[
                        column
                    ]
                ) for column in self.mock_columns
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            ),
            where=BinaryOperation(
                op='=',
                args=[
                    Identifier('Id'),
                    Constant('Id_value')
                ]
            ),
            limit=Constant(1)
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_conditions(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and multiple WHERE conditions.
        """
        select_query = Select(
            targets=[
                Identifier(
                    parts=[
                        column
                    ]
                ) for column in self.mock_columns
            ],
            from_table=Identifier(
                parts=[
                    self.table_name
                ]
            ),
            where=BinaryOperation(
                op='AND',
                args=[
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('Id'),
                            Constant('Id_value')
                        ]
                    ),
                    BinaryOperation(
                        op='=',
                        args=[
                            Identifier('Name'),
                            Constant('Name_value')
                        ]
                    )
                ]
            )
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    # TODO: Add tests for `add`, `modify`, and `remove` methods.


if __name__ == '__main__':
    unittest.main()

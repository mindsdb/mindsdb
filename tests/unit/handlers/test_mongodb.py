import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from bson import ObjectId
from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast.select.star import Star
import pymongo
from pymongo.errors import InvalidURI, OperationFailure
import pymongo.results

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.handlers.mongodb_handler.mongodb_handler import MongoDBHandler


class TestMongoDBHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            host='mongodb://localhost:27017',
            database='sample_mflix'
        )

    def create_handler(self):
        return MongoDBHandler('mongodb', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.mongodb_handler.mongodb_handler.MongoClient')

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that pymongo.MongoClient is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure_with_invalid_uri(self):
        """
        Test if `connect` method raises InvalidURI exception when an invalid URI is provided.
        """
        self.mock_connect.side_effect = InvalidURI

        with self.assertRaises(InvalidURI):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

    def test_connect_failure_with_incorrect_credentials(self):
        """
        Test if `connect` method raises OperationFailure exception when incorrect credentials are provided.
        """
        self.mock_connect.side_effect = OperationFailure(error='Authentication failed.')

        with self.assertRaises(OperationFailure):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

    def test_check_connection_failure_with_non_existent_database(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on failed connection due to non-existent database.
        """
        self.mock_connect.return_value.list_database_names.return_value = ['demo']

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_check_connection_success(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        self.mock_connect.return_value.list_database_names.return_value = ['sample_mflix']

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_query_failure_with_non_existent_collection(self):
        """
        Test if the `query` method returns a response object with an error message on failed query due to non-existent collection.
        """
        self.mock_connect.return_value[self.dummy_connection_data['database']].list_collection_names.return_value = ['movies']

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier('theaters')
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertTrue(response.error_message)

    def test_query_failure_with_unsupported_query_type(self):
        """
        Test if the `query` method raises NotImplementedError on unsupported query operation.
        This exception will be raised in the `to_mongo_query` method of the `MongodbRender` class.
        """
        query = ast.Insert(
            table=ast.Identifier('table1'),
            columns=['id', 'name'],
            values=[[1, 'Alice']]
        )

        with self.assertRaises(NotImplementedError):
            self.handler.query(query)

    def test_query_failure_with_unsupported_operation(self):
        """
        Test if the `query` method raises NotImplementedError on unsupported operation.
        This exception will be raised in the `handle_where` method of the `MongodbRender` class.
        """
        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier('movies'),
            where=ast.BinaryOperation(
                args=[
                    ast.Identifier('name'),
                    ast.Constant('The Dark Knight')
                ],
                op='in'
            )
        )

        with self.assertRaises(NotImplementedError):
            self.handler.query(query)

    def test_query_select_success(self):
        """
        Test if the `query` method returns a response object with a data frame containing the query result.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        self.mock_connect.return_value[self.dummy_connection_data['database']].list_collection_names.return_value = ['movies']

        self.mock_connect.return_value[self.dummy_connection_data['database']]['movies'].aggregate.return_value = [
            {
                '_id': ObjectId('5f5b3f3b3f3b3f3b3f3b3f3b'),
                'name': 'The Dark Knight',
                'plot': 'The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan.',
                'runtime': 152
            }
        ]

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier('movies')
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ['_id', 'name', 'plot', 'runtime'])
        self.assertEqual(df['name'].tolist(), ['The Dark Knight'])

    def test_query_update_success(self):
        """
        Test if the `query` method returns a response object with a 'OK' status.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        self.mock_connect.return_value[self.dummy_connection_data['database']].list_collection_names.return_value = ['movies']

        self.mock_connect.return_value[self.dummy_connection_data['database']]['movies'].update_many.return_value = pymongo.results.UpdateResult(
            acknowledged=True,
            raw_result={
                'n': 1,
                'nModified': 1
            }
        )

        query = ast.Update(
            table=ast.Identifier('movies'),
            update_columns={
                'name': ast.Constant('The Dark Knight'),
                'plot': ast.Constant('The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan.'),
                'runtime': ast.Constant(152)
            },
            where=ast.BinaryOperation(
                args=[
                    ast.Identifier('name'),
                    ast.Constant('The Dark Knight')
                ],
                op='='
            )
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)

    def test_get_tables(self):
        """
        Tests the `get_tables` method returns a response object with a list of tables (collections) in the database.
        """
        self.mock_connect.return_value[self.dummy_connection_data['database']].list_collection_names.return_value = ['theaters', 'movies', 'comments', 'sessions', 'users', 'embedded_movies']

        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 6)
        self.assertEqual(df.columns.tolist(), ['table_name'])
        self.assertEqual(df['table_name'].tolist(), ['theaters', 'movies', 'comments', 'sessions', 'users', 'embedded_movies'])

    def test_get_columns(self):
        """
        Tests the `get_columns` method returns a response object with a list of columns (fields) for a given table (collection).
        """
        self.mock_connect.return_value[self.dummy_connection_data['database']]['movies'].find_one.return_value = {
            '_id': ObjectId('5f5b3f3b3f3b3f3b3f3b3f3b'),
            'name': 'The Dark Knight',
            'plot': 'The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan.',
            'runtime': 152
        }

        response = self.handler.get_columns('movies')

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 4)
        self.assertEqual(df.columns.tolist(), ['Field', 'Type'])
        self.assertEqual(df['Field'].tolist(), ['_id', 'name', 'plot', 'runtime'])
        self.assertEqual(df['Type'].tolist(), ['str', 'str', 'str', 'int'])


if __name__ == '__main__':
    unittest.main()

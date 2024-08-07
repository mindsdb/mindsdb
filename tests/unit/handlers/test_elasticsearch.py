from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import elasticsearch

from base_handler_test import BaseDatabaseHandlerTest
from mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler import ElasticsearchHandler
from mindsdb.integrations.libs.response import HandlerResponse as Response


class TestElasticsearchHandler(BaseDatabaseHandlerTest, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            hosts='http://localhost:9200',
            user='example_user',
            password='example_pass'
        )

    @property
    def err_to_raise_on_connect_failure(self):
        return elasticsearch.exceptions.AuthenticationException("Connection Failed", 403)

    @property
    def get_tables_query(self):
        return """
            SHOW TABLES
        """

    @property
    def get_columns_query(self):
        return f"""
            DESCRIBE {self.mock_table}
        """

    def create_handler(self):
        return ElasticsearchHandler('elasticsearch', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('mindsdb.integrations.handlers.elasticsearch_handler.elasticsearch_handler.Elasticsearch')

    def test_native_query(self):
        """
        Tests the `native_query` method to ensure it executes a SQL query and returns a Response object.
        """
        mock_conn = MagicMock()
        mock_conn.sql.query = MagicMock(
            return_value={
                'rows': [[1, 2.0]],
                'columns': [
                    {'name': 'column1', 'type': 'integer'},
                    {'name': 'column2', 'type': 'float'},
                ],
            }
        )

        self.handler.connect = MagicMock(return_value=mock_conn)

        query_str = "SELECT * FROM table"
        data = self.handler.native_query(query_str)

        mock_conn.sql.query.assert_called_once_with(body={'query': query_str})
        assert isinstance(data, Response)
        self.assertFalse(data.error_code)


if __name__ == '__main__':
    unittest.main()

from collections import OrderedDict
import unittest
from unittest.mock import patch

from mysql.connector import Error as MySQLError

from mindsdb.integrations.handlers.mysql_handler.mysql_handler import MySQLHandler
from tests.unit.handlers.base_db_test import BaseDBTest


class TestMySQLHandler(BaseDBTest, unittest.TestCase):

    def setUp(self):
        self.dummy_connection_data = OrderedDict(
            host='127.0.0.1',
            port=3306,
            user='example_user',
            password='example_pass',
            database='example_db',
            url='mysql://example_user:example_pass@localhost:3306/example_db'
        )

        self.err_to_raise_on_connect_failure = MySQLError("Connection Failed")

        self.get_tables_query = """
            SELECT
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                TABLE_TYPE AS table_type
            FROM
                information_schema.TABLES
            WHERE
                TABLE_TYPE IN ('BASE TABLE', 'VIEW')
                AND TABLE_SCHEMA = DATABASE()
            ORDER BY 2
            ;
        """

        self.get_columns_query = f"DESCRIBE `{self.mock_table}`;"

        return super().setUp()

    def create_handler(self):
        return MySQLHandler('mysql', connection_data=self.dummy_connection_data)
    
    def create_patcher(self):
        return patch('mysql.connector.connect')


if __name__ == '__main__':
    unittest.main()

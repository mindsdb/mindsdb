"""
Get env ready with ease leveraging nebulagraph-lite.

ref: https://github.com/wey-gu/nebulagraph-lite

$pip3 install nebulagraph-lite
$nebulagraph start
"""

import unittest
import pandas as pd
from mindsdb.integrations.handlers.nebulagraph_handler import (
    Handler as NebulaGraphHandler,
)
from mindsdb.integrations.libs.base import DatabaseHandler

from typing import Dict, Any


class TestNebulaGraphHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data: Dict[str, Any] = {
            "host": "127.0.0.1",
            "port": 9669,
            "session_pool_size": 10,
            "graph_space": "basketballplayer",
            "user": "root",
            "password": "nebula",
        }

        cls.handler: DatabaseHandler = NebulaGraphHandler(
            "test_nebulagraph_handler", connection_data=connection_data
        )

    def test_connect(self):
        self.handler.connect()
        self.assertIsNotNone(self.handler.connection)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection(self):
        self.handler.connect()
        response = self.handler.check_connection()
        self.assertTrue(response.success)

    def test_native_query(self):
        query = "SHOW HOSTS;"
        response = self.handler.native_query(query)
        self.assertEqual(response.error_code, 0)

        query = (
            "MATCH ()-[e:serve]->() "
            "RETURN src(e) AS src, "
            "dst(e) AS dst, "
            "e.start_year AS start_year, "
            "e.end_year AS end_year "
            "LIMIT 3;"
        )
        response = self.handler.native_query(query)
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 3)

        query = (
            "FIND SHORTEST PATH WITH PROP FROM 'team204' TO 'player100' "
            "OVER * REVERSELY YIELD path AS p;"
        )
        response = self.handler.native_query(query)
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 1)

    def test_get_tables(self):
        response = self.handler.get_tables()
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 4)

        response = self.handler.get_tables()
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        expected_tables = ["player", "team", "serve", "follow"]
        expected_types = ["TAG", "TAG", "EDGE", "EDGE"]
        for table, t_type in zip(expected_tables, expected_types):
            self.assertIn(table, response.data_frame["Name"].values)
            self.assertIn(t_type, response.data_frame["table_type"].values)

    def test_get_columns(self):
        response = self.handler.get_columns("player")
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.assertEqual(response.data_frame["Field"].values[0], "name")
        self.assertEqual(response.data_frame["Type"].values[0], "string")
        self.assertEqual(response.data_frame["Null"].values[0], "YES")
        self.assertEqual(response.data_frame["Default"].values[0], None)

        response = self.handler.get_columns("serve")
        self.assertEqual(response.error_code, 0)
        self.assertEqual(response.type, "table")
        self.assertIsInstance(response.data_frame, pd.DataFrame)
        self.assertEqual(len(response.data_frame), 2)
        self.assertEqual(response.data_frame["Field"].values[0], "start_year")
        self.assertEqual(response.data_frame["Type"].values[0], "int64")
        self.assertEqual(response.data_frame["Null"].values[0], "YES")
        self.assertEqual(response.data_frame["Default"].values[0], None)

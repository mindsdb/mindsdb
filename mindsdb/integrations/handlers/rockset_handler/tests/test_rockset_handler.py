import unittest
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.rockset_handler.rockset_handler import RocksetHandler


class RocksetHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_args": {
                "host": '127.0.0.1',
                "port": 3306,
                "user": "rockset",
                "password": "rockset"
            }
        }
        cls.handler = RocksetHandler('test_rockset_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        self.assertEqual(tables, [])

    def test_2_get_columns(self):
        columns = self.handler.get_columns('test')
        self.assertEqual(columns, [])

    def test_3_query(self):
        response = self.handler.query('SELECT 1')
        self.assertEqual(response['type'], RESPONSE_TYPE.QUERY)
        self.assertEqual(response['data'], [[1]])

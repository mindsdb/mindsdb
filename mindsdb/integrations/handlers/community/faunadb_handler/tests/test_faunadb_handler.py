import unittest
from mindsdb_sql_parser import parse_sql

from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.faunadb_handler.faunadb_handler import (
    FaunaDBHandler,
)


class FaunadbHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "fauna_secret": "fnAFQFQPZNAAUYkCYkdvozJsm9tH2VbX55AULhsH",
                "fauna_endpoint": "https://db.fauna.com:443/",
            }
        }
        cls.handler = FaunaDBHandler("test_faunadb_handler", **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()
        self.assertTrue(self.handler.is_connected)

    def test_2_select(self):
        query = parse_sql("SELECT * FROM books;")
        res = self.handler.query(query)
        assert res.type is RESPONSE_TYPE.TABLE

    def test_3_describe_db(self):
        res = self.handler.get_tables()
        assert res.type is RESPONSE_TYPE.TABLE


if __name__ == "__main__":
    unittest.main()

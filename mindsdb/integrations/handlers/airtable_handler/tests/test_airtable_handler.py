import unittest
from mindsdb.integrations.handlers.airtable_handler.airtable_handler import (
    AirtableHandler,
)
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class AirtableHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "base_id": "dqweqweqrwwqq",
            "table_name": "iris",
            "api_key": "knlsndlknslk",
        }
        cls.handler = AirtableHandler("test_airtable_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM iris"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns()
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

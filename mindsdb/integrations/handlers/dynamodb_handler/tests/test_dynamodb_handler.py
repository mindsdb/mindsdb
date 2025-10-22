import unittest
from mindsdb.integrations.handlers.dynamodb_handler.dynamodb_handler import DyanmoDBHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class DynamoDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "aws_access_key_id": "PCAQ2LJDOSWLNSQKOCPW",
            "aws_secret_access_key": "U/VjewPlNopsDmmwItl34r2neyC6WhZpUiip57i",
            "region_name": "us-east-1",
        }
        cls.handler = DyanmoDBHandler("test_dynamodb_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM TryDaxTable"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns("TryDaxTable")
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

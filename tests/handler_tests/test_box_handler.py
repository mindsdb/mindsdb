import unittest
from mindsdb.integrations.handlers.box_handler.box_handler import BoxHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class BoxHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "client_id": "YOUR_CLIENT_ID",
            "client_secret": "YOUR_CLIENT_SECRET",
            "access_token": "YOUR_ACCESS_TOKEN",
            "refresh_token": "YOUR_REFRESH_TOKEN",
        }
        cls.handler = BoxHandler("test_box_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM files"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_3_get_columns(self):
        columns = self.handler.get_columns("files")
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

import unittest
from mindsdb.integrations.handlers.databricks_handler.databricks_handler import (
    DatabricksHandler,
)
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class DatabricksHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "server_hostname": "adb-1234567890123456.7.azuredatabricks.net",
            "http_path": "sql/protocolv1/o/1234567890123456/1234-567890-test123",
            "access_token": "dapi1234567890ab1cde2f3ab456c7d89efa",
            "schema": "sales",
        }
        cls.handler = DatabricksHandler("test_databricks_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_native_query_select(self):
        query = "SELECT * FROM sales_features"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_2_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_4_get_columns(self):
        columns = self.handler.get_columns("sales_features")
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

import unittest

from mindsdb.integrations.handlers.couchbase_handler.couchbase_handler import (
    CouchbaseHandler,
)
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class CouchbaseHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        connection_data = {
            "host": "192.168.33.10",
            "user": "admin",
            "password": "00154abs",
            "bucket": "bag-bucket",
            "scope": "test-scope",  # This is optinal, but if ommited will default to _default.
        }
        cls.kwargs = dict(connection_data=connection_data)
        cls.handler = CouchbaseHandler("test_couchbase_handler", **cls.kwargs)

    def test_0_connect(self):
        self.handler.check_connection()

    def test_1_get_tables(self):
        tbls = self.handler.get_tables()
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_2_get_column(self):
        tbls = self.handler.get_columns("onsale")
        assert tbls.type is not RESPONSE_TYPE.ERROR

    def test_3_native_query_select(self):
        tbls = self.handler.native_query("SELECT * FROM onsale")
        assert tbls.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

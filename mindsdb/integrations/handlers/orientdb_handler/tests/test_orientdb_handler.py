import unittest

from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE
from mindsdb.integrations.handlers.orientdb_handler.orientdb_handler import (
    OrientDBHandler,
)


class OrientDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": 2424,
                "user": "root",
                "password": "root",
                "database": "demo",
            }
        }
        cls.handler = OrientDBHandler("test_orientdb_handler", **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_drop_table(self):
        res = self.handler.native_query("DROP CLASS integers")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_create_table(self):
        res = self.handler.native_query(
            """
            CREATE CLASS integers; 
            CREATE PROPERTY integers.i INTEGER;
            """
        )
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_4_insert_into_table(self):
        res = self.handler.native_query("INSERT INTO (i) integers VALUES (42)")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_5_select(self):
        res = self.handler.native_query("SELECT * FROM integers")
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_describe_table(self):
        res = self.handler.get_columns("integers")
        assert res.type is RESPONSE_TYPE.TABLE

    def test_7_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

import unittest
from mindsdb.integrations.handlers.db2_handler.db2_handler import DB2Handler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class DB2HandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "127.0.0.1",
                "port": "25000",
                "user": "db2admin",
                "password": "1234",
                "database": "Books",
                "schema_name": "db2admin"
            }
        }
        cls.handler = DB2Handler('test_db2_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_drop_table(self):
        res = self.handler.query("DROP TABLE IF EXISTS LOVE")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_2_create_table(self):
        res = self.handler.query("CREATE TABLE IF NOT EXISTS LOVE (LOVER varchar(20))")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR
 
    def test_4_select_query(self):
        query = "SELECT * FROM AUTHORS"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_check_connection(self):
         self.handler.check_connection()

        
if __name__ == '__main__':
    unittest.main()
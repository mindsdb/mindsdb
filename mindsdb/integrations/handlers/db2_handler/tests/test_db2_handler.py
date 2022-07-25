import unittest
from mindsdb.integrations.handlers.db2_handler.db2_handler import DB2Handler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class DB2HandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "127.0.0.1",
            "port": "25000",
            "user": "db2admin",
            "password": "1234",
            "database": "Books",
            "schemaName": "db2admin"
        }
        cls.handler = DB2Handler('test_db2_handler', **cls.kwargs)

    def test_0_connect(self):
         self.handler.check_connection()

    

    def test_1_drop_table(self):
        res = self.handler.native_query("DROP TABLE IF EXISTS test_mdb")
        assert res['type'] is not RESPONSE_TYPE.ERROR 


    def test_2_create_table(self):
        res = self.handler.native_query("CREATE TABLE IF NOT EXISTS test_mdb (test_col INT)")
        assert res['type'] is not RESPONSE_TYPE.ERROR 

    def test_3_get_tables(self):
        tables = self.handler.get_tables()
        assert tables['type'] is not RESPONSE_TYPE.ERROR

 
    def test_4_select_query(self):
        query = "SELECT * FROM test_mdb"
        result = self.handler.native_query(query)
        assert result['type'] is RESPONSE_TYPE.TABLE

        
if __name__ == '__main__':
    unittest.main()
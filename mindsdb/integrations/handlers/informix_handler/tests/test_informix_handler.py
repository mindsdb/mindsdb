import unittest
from mindsdb.integrations.handlers.informix_handler.informix_handler import InformixHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class InformixHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "server": "server",
                "host": "127.0.0.1",
                "port": 9093,
                "user": "informix",
                "password": "in4mix",
                "database": "demo",
                "schema_name": "love",
                "loging_enabled": False
            }
        }
        cls.handler = InformixHandler('test_informix_handler', cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_drop_table(self):
        res = self.handler.query("DROP TABLE IF EXISTS LOVE;")
        assert res.type is  not RESPONSE_TYPE.ERROR

    def test_2_create_table(self):
        res = self.handler.query("CREATE TABLE IF NOT EXISTS LOVE (LOVER varchar(20));")
        assert res.type is not  RESPONSE_TYPE.ERROR 
    
    def test_3_insert(self):
        res = self.handler.query("INSERT INTO LOVE VALUES('Hari');")
        assert res.type is not RESPONSE_TYPE.ERROR
    
    def test_4_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is RESPONSE_TYPE.TABLE
 
    def test_5_select_query(self):
        query = "SELECT * FROM LOVE;"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_5_check_connection(self):
        self.handler.check_connection()

        
if __name__ == '__main__':
    unittest.main()
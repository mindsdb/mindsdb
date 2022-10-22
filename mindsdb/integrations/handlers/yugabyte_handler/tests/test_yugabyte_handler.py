import unittest
from mindsdb.integrations.handlers.yugabyte_handler.yugabyte_handler import YugabyteHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE


class YugabyteHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": 5433,
                "user": "admin",
                "password": "",
                "database": "yugabyte"
            }
        }
        cls.handler = YugabyteHandler('test_yugabyte_handler', cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_drop_table(self):
        res = self.handler.query("DROP TABLE IF EXISTS PREM;")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_2_create_table(self):
        res = self.handler.query("CREATE TABLE IF NOT EXISTS PREM (Premi varchar(50));")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_insert_table(self):
        res = self.handler.query("INSERT INTO PREM VALUES('Radha <3 Krishna');")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_4_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR
 
    def test_5_select_query(self):
        query = "SELECT * FROM PREM;"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE or RESPONSE_TYPE.OK

    def test_6_check_connection(self):
        self.handler.check_connection()

        
if __name__ == '__main__':
    unittest.main()
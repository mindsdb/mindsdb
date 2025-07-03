import unittest
from mindsdb.integrations.handlers.nuo_jdbc_handler.nuo_jdbc_handler import NuoHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class NuoHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": 48006,
                "database": "test",
                "schema": "hockey",
                "user": "dba",
                "password": "goalie",
                "is_direct": "true",
            }
        }
        cls.handler = NuoHandler('test_nuo_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_create(self):
        res = self.handler.query('CREATE TABLE TESTTABLEX3 (ID INT PRIMARY KEY, NAME VARCHAR(14))')
        assert res.type is RESPONSE_TYPE.OK

    def test_3_insert(self):
        res = self.handler.query("INSERT INTO TESTTABLEX3 VALUES (100,'ONE HUNDRED'),(200,'TWO HUNDRED'),(300,'THREE HUNDRED')")
        assert res.type is RESPONSE_TYPE.OK

    def test_4_select(self):
        res = self.handler.query('SELECT * FROM HOCKEY')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_5_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_get_columns(self):
        res = self.handler.get_columns("HOCKEY")
        assert res.type is RESPONSE_TYPE.TABLE


if __name__ == '__main__':
    unittest.main()

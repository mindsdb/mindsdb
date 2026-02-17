import unittest

from mindsdb.integrations.handlers.d0lt_handler.d0lt_handler import D0ltHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class D0ltHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "",
            "database": "getting_started",
            "ssl": False,
        }
        cls.handler = D0ltHandler("test_mysql_handler", connection_data=cls.kwargs)

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
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_6_get_columns(self):
        result = self.handler.get_columns("PREM")
        assert result.type is not RESPONSE_TYPE.ERROR

    def test_7_check_connection(self):
        self.handler.check_connection()


if __name__ == "__main__":
    unittest.main()

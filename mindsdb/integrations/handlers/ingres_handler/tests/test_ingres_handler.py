import unittest
from mindsdb.integrations.handlers.ingres_handler.ingres_handler import IngresHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class IngresHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "user": "admin",
                "password": "password",
                "server": "(local)",
                "database": "test_db"
            }
        }
        cls.handler = IngresHandler('test_ingres_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_2_select_query(self):
        query = "SELECT * FROM test_db.home_rentals"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_3_get_columns(self):
        columns = self.handler.get_columns('test')
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_4_drop_table(self):
        res = self.handler.native_query("DROP TABLE IF EXISTS test_db.test")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_5_create_table(self):
        res = self.handler.native_query("CREATE TABLE IF NOT EXISTS test_db.test (id INT, name VARCHAR(255))")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_6_insert(self):
        res = self.handler.native_query("INSERT INTO test VALUES (100,'ONE HUNDRED')")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_7_disconnect(self):
        assert self.handler.disconnect()


if __name__ == '__main__':
    unittest.main()

import unittest

from mindsdb.integrations.handlers.surrealdb_handler.surrealdb_handler import SurrealDBHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class SurrealdbHandlerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "connection_data": {
                "host": "localhost",
                "port": "8000",
                "user": "admin",
                "password": "password",
                "namespace": "test",
                "database": "test"
            }
        }
        cls.handler = SurrealDBHandler('test_surrealdb_handler', **cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_create_table(self):
        res = self.handler.native_query("CREATE person SET name = 'Tobie', company = 'SurrealDB', "
                                        "skills = ['Rust', 'Go', 'JavaScript'];")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_2_insert(self):
        res = self.handler.native_query("UPDATE person SET name = 'Jamie'")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_select_query(self):
        query = "SELECT * FROM person"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_columns(self):
        columns = self.handler.get_columns('person')
        assert columns.type is not RESPONSE_TYPE.ERROR

    def test_5_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_6_drop_table(self):
        res = self.handler.native_query("REMOVE table person")
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_7_disconnect(self):
        assert self.handler.disconnect() is None


if __name__ == '__main__':
    unittest.main()

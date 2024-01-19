import unittest
from mindsdb.api.executor.data_types.response_type import (
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.duckdb_handler.duckdb_handler import (
    DuckDBHandler,
)


class DuckDBHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {'connection_data': {'database': 'db.duckdb'}}
        cls.handler = DuckDBHandler('test_duckdb_handler', **cls.kwargs)

    def test_0_connect(self):
        self.handler.connect()

    def test_1_check_connection(self):
        self.handler.check_connection()

    def test_2_drop_table(self):
        res = self.handler.query('DROP TABLE IF EXISTS integers;')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_3_create_table(self):
        res = self.handler.query('CREATE TABLE integers(i INTEGER)')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_4_insert_into_table(self):
        res = self.handler.query('INSERT INTO integers VALUES (42)')
        assert res.type is not RESPONSE_TYPE.ERROR

    def test_5_select(self):
        res = self.handler.query('SELECT * FROM integers;')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_6_describe_table(self):
        res = self.handler.get_columns('integers')
        assert res.type is RESPONSE_TYPE.TABLE

    def test_7_get_tables(self):
        res = self.handler.get_tables()
        assert res.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()

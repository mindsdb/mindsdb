import unittest
from mindsdb.integrations.handlers.libsql_handler.libsql_handler import LibSQLHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class LibSQLHandlerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.kwargs = {
            "database": "tests/test.db",
        }
        cls.handler = LibSQLHandler("test_libsql_handler", cls.kwargs)

    def test_0_check_connection(self):
        assert self.handler.check_connection()

    def test_1_create_table(self):
        query = "CREATE TABLE IF NOT EXISTS user (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.OK

    def test_2_insert_rows(self):
        query = (
            "INSERT OR IGNORE INTO user (name, age) VALUES ('Alice', 30), ('Bob', 25)"
        )
        result = self.handler.native_query(query)
        print(result)
        assert result.type is RESPONSE_TYPE.OK

    def test_3_native_query_select(self):
        query = "SELECT * FROM user"
        result = self.handler.native_query(query)
        assert result.type is RESPONSE_TYPE.TABLE

    def test_4_get_tables(self):
        tables = self.handler.get_tables()
        assert tables.type is not RESPONSE_TYPE.ERROR

    def test_5_get_columns(self):
        columns = self.handler.get_columns("user")
        assert columns.type is not RESPONSE_TYPE.ERROR


if __name__ == "__main__":
    unittest.main()

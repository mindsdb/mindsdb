import pytest

from mindsdb.integrations.handlers.apache_doris_handler.apache_doris_handler import ApacheDorisHandler
from mindsdb.api.mysql.mysql_proxy.libs.constants.response_type import RESPONSE_TYPE

HANDLER_KWARGS = {
    "connection_data": {
        "host": "127.0.0.1",
        "port": 9030,
        "user": "root",
        "password": "password",
        "database": "mindstest"
    }
}

HANDLER_NAME = 'test_doris_handler'


@pytest.fixture(scope="module")
def handler(request):
    handler = ApacheDorisHandler(HANDLER_NAME, **HANDLER_KWARGS)
    yield handler


class TestMySQLHandler:

    def check_valid_response(self, res):
        if res.resp_type == RESPONSE_TYPE.TABLE:
            assert res.data_frame is not None, "expected to have some data, but got None"
        assert res.error_code == 0, f"expected to have zero error_code, but got {res.error_code}"
        assert res.error_message is None, f"expected to have None in error message, but got {res.error_message}"

    def get_table_names(self, handler):
        res = handler.get_tables()
        tables = res.data_frame
        assert tables is not None, "expected to have some tables in the db, but got None"
        assert 'table_name' in tables, f"expected to get 'table_name' column in the response:\n{tables}"
        return list(tables['table_name'])

    def test_connect(self, handler):
        handler.connect()
        assert handler.is_connected, "connection error"

    def test_check_connection(self, handler):
        res = handler.check_connection()
        assert res.success, res.error_message

    def test_native_query_show_dbs(self, handler):
        dbs = handler.native_query("SHOW DATABASES;")
        dbs = dbs.data_frame
        assert dbs is not None, "expected to get some data, but got None"
        assert 'Database' in dbs, f"expected to get 'Database' column in response:\n{dbs}"
        dbs = list(dbs["Database"])
        expected_db = HANDLER_KWARGS["connection_data"]["database"]
        assert expected_db in dbs, f"expected to have {expected_db} db in response: {dbs}"

    def test_get_tables(self, handler):
        tables = self.get_table_names(handler)
        assert "example_tbl" in tables, f"expected to have 'example_tbl' table in the db but got: {tables}"

    def test_describe_table(self, handler):
        described = handler.get_columns("example_tbl")
        describe_data = described.data_frame
        self.check_valid_response(described)
        got_columns = list(describe_data.iloc[:, 0])
        want_columns = ["user_id", "date", "city", "age", "sex", "last_visit_date", "cost", "max_dwell_time", "min_dwell_time"]
        assert got_columns == want_columns, f"expected to have next columns in table:\n{want_columns}\nbut got:\n{got_columns}"

    def test_create_table(self, handler):
        new_table = "test_mdb"
        res = handler.native_query(f"""
            CREATE TABLE IF NOT EXISTS {new_table} (test_col INT)
            DISTRIBUTED BY HASH(test_col) BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            );
        """)
        self.check_valid_response(res)
        tables = self.get_table_names(handler)
        assert new_table in tables, f"expected to have {new_table} in database, but got: {tables}"

    def test_drop_table(self, handler):
        drop_table = "test_mdb"
        res = handler.native_query(f"DROP TABLE IF EXISTS {drop_table}")
        self.check_valid_response(res)
        tables = self.get_table_names(handler)
        assert drop_table not in tables

    def test_select_query(self, handler):
        limit = 3
        query = f"SELECT * FROM example_tbl LIMIT {limit}"
        res = handler.query(query)
        self.check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert got_rows == want_rows, f"expected to have {want_rows} rows in response but got: {got_rows}"

    def test_select_where_query(self, handler):
        want_rows = 5
        query = "SELECT * FROM example_tbl WHERE sex = 0"
        res = handler.query(query)
        self.check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        assert got_rows == want_rows, f"expected to have {want_rows} rows in response but got: {got_rows}"

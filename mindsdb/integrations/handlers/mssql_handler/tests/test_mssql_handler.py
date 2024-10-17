import os
import pytest
import pymssql
import pandas as pd
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

HANDLER_KWARGS = {
    "connection_data": {
        "host": os.environ.get("MDB_TEST_MSSQL_HOST", "localhost"),
        "port": os.environ.get("MDB_TEST_MSSQL_PORT", "1433"),
        "user": os.environ.get("MDB_TEST_MSSQL_USER", "sa"),
        "password": os.environ.get("MDB_TEST_MSSQL_PASSWORD", "admin5678@"),
        "database": os.environ.get("MDB_TEST_MSSQL_DATABASE", "mdb_db_handler_test"),
    }
}


@pytest.fixture(scope="class")
def sql_server_handler():
    seed_db()
    handler = SqlServerHandler("test_sqlserver_handler", **HANDLER_KWARGS)
    yield handler
    handler.disconnect()


def seed_db():
    """Seed the test DB with some data"""

    # Connect to 'master' while we create our test DB
    conn_info = HANDLER_KWARGS["connection_data"].copy()
    conn_info["database"] = "master"
    db = pymssql.connect(**conn_info, autocommit=True)
    cursor = db.cursor()

    with open("mindsdb/integrations/handlers/mssql_handler/tests/seed.sql", "r") as f:
        for line in f.readlines():
            cursor.execute(line)
    cursor.close()
    db.close()


def check_valid_response(res):
    if res.resp_type == RESPONSE_TYPE.TABLE:
        assert res.data_frame is not None, "expected to have some data, but got None"
    assert (
        res.error_code == 0
    ), f"expected to have zero error_code, but got {res.error_code}"
    assert (
        res.error_message is None
    ), f"expected to have None in error message, but got {res.error_message}"


def get_table_names(sql_server_handler):
    res = sql_server_handler.get_tables()
    tables = res.data_frame
    assert tables is not None, "expected to have some tables in the db, but got None"
    assert (
        "table_name" in tables
    ), f"expected to get 'table_name' column in the response:\n{tables}"
    return list(tables["table_name"])


@pytest.mark.usefixtures("sql_server_handler")
class TestMSSQLHandlerConnect:
    def test_connect(self, sql_server_handler):
        sql_server_handler.connect()
        assert sql_server_handler.is_connected, "the handler has failed to connect"

    def test_check_connection(self, sql_server_handler):
        res = sql_server_handler.check_connection()
        assert res.success, res.error_message


class TestMSSQLHandlerDisconnect:
    def test_disconnect(self, sql_server_handler):
        sql_server_handler.disconnect()
        assert sql_server_handler.is_connected is False, "failed to disconnect"

    def test_check_connection(self, sql_server_handler):
        res = sql_server_handler.check_connection()
        assert res.success, res.error_message


@pytest.mark.usefixtures("sql_server_handler")
class TestMSSQLHandlerTables:
    table_for_creation = "test_mdb"

    def test_get_tables(self, sql_server_handler):
        res = sql_server_handler.get_tables()
        tables = res.data_frame
        assert (
            tables is not None
        ), "expected to have some tables in the db, but got None"
        assert (
            "table_name" in tables
        ), f"expected to get 'table_name' in the response but got: {tables}"
        assert (
            "test" in tables["table_name"].values
        ), "expected to have 'test' in the response."

    def test_get_columns(self, sql_server_handler):
        response = sql_server_handler.get_columns("test")
        assert response.type == RESPONSE_TYPE.TABLE, "expected a TABLE"
        assert len(response.data_frame) > 0, "expected > O columns"

        expected_columns = {
            "Field": ["col_one", "col_two", "col_three", "col_four"],
            "Type": ["int", "int", "float", "varchar"],
        }
        expected_df = pd.DataFrame(expected_columns)
        assert response.data_frame.equals(
            expected_df
        ), "response does not contain the expected columns"

    def test_create_table(self, sql_server_handler):
        query = f"""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{self.table_for_creation}')
        BEGIN
            CREATE TABLE {self.table_for_creation} (test_col INT)
        END
        """
        res = sql_server_handler.native_query(query)
        check_valid_response(res)
        tables = get_table_names(sql_server_handler)
        assert (
            self.table_for_creation in tables
        ), f"expected to have {self.table_for_creation} in database, but got: {tables}"

    def test_drop_table(self, sql_server_handler):
        query = f"DROP TABLE IF EXISTS {self.table_for_creation}"
        res = sql_server_handler.native_query(query)
        check_valid_response(res)
        tables = get_table_names(sql_server_handler)
        assert self.table_for_creation not in tables


@pytest.mark.usefixtures("sql_server_handler")
class TestMSSQLHandlerQuery:
    def test_select_native_query(self, sql_server_handler):
        query = "SELECT * FROM test"
        response = sql_server_handler.native_query(query)
        assert type(response) is Response
        assert response.resp_type == RESPONSE_TYPE.TABLE

        expected_data = {
            "col_one": [1, 2, 3],
            "col_two": [-1, -2, -3],
            "col_three": [0.1, 0.2, 0.3],
            "col_four": ["A", "B", "C"],
        }
        expected_df = pd.DataFrame(expected_data)
        assert response.data_frame.equals(
            expected_df
        ), "response does not contain the expected data"

    def test_select_query(self, sql_server_handler):
        limit = 3
        query = "SELECT * FROM test"
        res = sql_server_handler.query(query)
        check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert (
            got_rows == want_rows
        ), f"expected to have {want_rows} rows in response but got: {got_rows}"

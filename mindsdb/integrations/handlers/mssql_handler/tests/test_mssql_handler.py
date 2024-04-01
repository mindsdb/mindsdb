import pytest
import pandas as pd
from unittest.mock import MagicMock
from mindsdb.integrations.handlers.mssql_handler.mssql_handler import SqlServerHandler
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)


@pytest.fixture(scope="class")
def sql_server_handler():
    HANDLER_KWARGS = {
        "connection_data": {
            "host": "localhost",
            "port": "1433",
            "user": "sa",
            "password": "admin5678@",
            "database": "master",
        }
    }
    handler = SqlServerHandler("test_sqlserver_handler", **HANDLER_KWARGS)
    yield handler
    handler.disconnect()


@pytest.fixture(scope="class")
def database_connection_and_cursor():
    connection = MagicMock()
    cursor = MagicMock()
    connection.cursor.return_value = cursor
    return connection, cursor


expected_columns = {
    "Field": ["col_one", "col_two", "col_three", "col_four"],
    "Type": ["int", "int", "float", "text"],
}

expected_data = {
    "col_one": [1, 2, 3],
    "col_two": [-1, -2, -3],
    "col_three": [0.1, 0.2, 0.3],
    "col_four": ["A", "B", "C"],
}


def check_valid_response(res):
    if res.resp_type == RESPONSE_TYPE.TABLE:
        assert res.data_frame is not None, "expected to have some data, but got None"
    assert (
        res.error_code == 0
    ), f"expected to have zero error_code, but got {res.error_code}"
    assert (
        res.error_message is None
    ), f"expected to have None in error message, but got {res.error_message}"


@pytest.mark.usefixtures("sql_server_handler", "database_connection_and_cursor")
class TestMssqlHandlerConnect:
    def test_connect(self, sql_server_handler):
        sql_server_handler.connect()
        assert sql_server_handler.is_connected, "the handler has failed to connect"


@pytest.mark.usefixtures("sql_server_handler", "database_connection_and_cursor")
class TestMssqlHandlerGet:
    def test_get_tables(self, sql_server_handler):
        tables = sql_server_handler.get_tables()
        res = sql_server_handler.get_tables()
        tables = res.data_frame
        assert (
            tables is not None
        ), "expected to have some tables in the db, but got None"
        assert (
            "table_name" in tables
        ), f"expected to get 'table_name' in the response but got: {tables}"

    def test_get_columns(self, sql_server_handler):
        response = sql_server_handler.get_columns("test")
        assert response.type == RESPONSE_TYPE.TABLE, f"expected a TABLE"
        assert len(response.data_frame) > 0, "expected > O columns"
        expected_df = pd.DataFrame(expected_columns)
        assert response.data_frame.equals(
            expected_df
        ), "response does not contain the expected columns"


@pytest.mark.usefixtures("sql_server_handler", "database_connection_and_cursor")
class TestMssqlHandlerQuery:
    def test_select_native_query(self, sql_server_handler):
        query = "SELECT * FROM test"
        response = sql_server_handler.native_query(query)
        assert type(response) is Response
        assert response.resp_type == RESPONSE_TYPE.TABLE
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

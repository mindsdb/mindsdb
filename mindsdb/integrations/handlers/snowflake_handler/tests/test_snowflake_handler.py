import os
import pytest
import snowflake
import pandas as pd
import snowflake.connector
from mindsdb.integrations.handlers.snowflake_handler.snowflake_handler import SnowflakeHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

HANDLER_KWARGS = {
    "connection_data": {
        "account": os.environ.get("MDB_TEST_SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("MDB_TEST_SNOWFLAKE_USER"),
        "password": os.environ.get("MDB_TEST_SNOWFLAKE_PASSWORD"),
        "database": os.environ.get("MDB_TEST_SNOWFLAKE_DATABASE"),
        "schema": os.environ.get("MDB_TEST_SNOWFLAKE_SCHEMA", "PUBLIC"),
    }
}


@pytest.fixture(scope="class")
def snowflake_handler():
    """
    Create a SnowflakeHandler instance for testing.
    """

    seed_db()
    handler = SnowflakeHandler("test_snowflake_handler", **HANDLER_KWARGS)
    yield handler
    handler.disconnect()


def seed_db():
    """
    Seed the test DB by running the queries in the seed.sql file.
    """

    # Connect to the SNOWFLAKE database to run seed queries
    conn_info = HANDLER_KWARGS["connection_data"].copy()
    conn_info["database"] = "SNOWFLAKE"
    db = snowflake.connector.connect(**conn_info)
    cursor = db.cursor()

    with open("mindsdb/integrations/handlers/snowflake_handler/tests/seed.sql", "r") as f:
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


def get_table_names(snowflake_handler):
    res = snowflake_handler.get_tables()
    tables = res.data_frame
    assert tables is not None, "expected to have some tables in the db, but got None"
    assert (
        "table_name" in tables
    ), f"expected to get 'table_name' column in the response:\n{tables}"
    return list(tables["table_name"])


@pytest.mark.usefixtures("snowflake_handler")
class TestSnowflakeHandlerConnect:
    def test_connect(self, snowflake_handler):
        snowflake_handler.connect()
        assert snowflake_handler.is_connected, "the handler has failed to connect"

    def test_check_connection(self, snowflake_handler):
        res = snowflake_handler.check_connection()
        assert res.success, res.error_message


@pytest.mark.usefixtures("snowflake_handler")
class TestSnowflakeHandlerTables:
    table_for_creation = "TEST_MDB"

    def test_get_tables(self, snowflake_handler):
        res = snowflake_handler.get_tables()
        tables = res.data_frame
        assert (
            tables is not None
        ), "expected to have some tables in the db, but got None"
        assert (
            "table_name" in tables
        ), f"expected to get 'table_name' in the response but got: {tables}"
        assert (
            "TEST" in tables["table_name"].values
        ), "expected to have 'test' in the response."

    def test_get_columns(self, snowflake_handler):
        response = snowflake_handler.get_columns("test")
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

    def test_create_table(self, snowflake_handler):
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_for_creation} (
                test_col INT
            );
        """
        res = snowflake_handler.native_query(query)
        check_valid_response(res)
        tables = get_table_names(snowflake_handler)
        assert (
            self.table_for_creation in tables
        ), f"expected to have {self.table_for_creation} in database, but got: {tables}"

    def test_drop_table(self, snowflake_handler):
        query = f"DROP TABLE IF EXISTS {self.table_for_creation}"
        res = snowflake_handler.native_query(query)
        check_valid_response(res)
        tables = get_table_names(snowflake_handler)
        assert self.table_for_creation not in tables

@pytest.mark.usefixtures("snowflake_handler")
class TestSnowflakeHandlerQuery:
    def test_select_native_query(self, snowflake_handler):
        query = "SELECT * FROM test"
        response = snowflake_handler.native_query(query)
        assert type(response) is Response
        assert response.resp_type == RESPONSE_TYPE.TABLE

        expected_data = {
            "COL_ONE": [1, 2, 3],
            "COL_TWO": [-1, -2, -3],
            "COL_THREE": [0.1, 0.2, 0.3],
            "COL_FOUR": ["A", "B", "C"],
        }
        expected_df = pd.DataFrame(expected_data)
        assert response.data_frame.equals(
            expected_df
        ), "response does not contain the expected data"

    def test_select_query(self, snowflake_handler):
        limit = 3
        query = "SELECT * FROM test"
        res = snowflake_handler.query(query)
        check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert (
            got_rows == want_rows
        ), f"expected to have {want_rows} rows in response but got: {got_rows}"


if __name__ == "__main__":
    pytest.main([__file__])
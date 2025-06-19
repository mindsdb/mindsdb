import os
import pytest
import pandas as pd
from google.cloud import bigquery

from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler
from mindsdb.integrations.utilities.handlers.auth_utilities.google import GoogleServiceAccountOAuth2Manager
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    RESPONSE_TYPE,
)

HANDLER_KWARGS = {
    "connection_data": {
        "project_id": os.environ.get("MDB_TEST_BIGQUERY_PROJECT_ID"),
        "dataset": os.environ.get("MDB_TEST_BIGQUERY_DATASET"),
        "service_account_json": os.environ.get("MDB_TEST_BIGQUERY_SERVICE_ACCOUNT_JSON"),
    }
}


@pytest.fixture(scope="class")
def bigquery_handler():
    """
    Create a BigQuery instance for testing.
    """
    seed_db()
    handler = BigQueryHandler("test_bigquery_handler", **HANDLER_KWARGS)
    yield handler
    handler.disconnect()


def seed_db():
    """
    Seed the test DB by running the queries in the seed.sql file.
    """
    # Connect to the BigQuery warehouse to run seed queries
    google_sa_oauth2_manager = GoogleServiceAccountOAuth2Manager(
        credentials_json=HANDLER_KWARGS["connection_data"]["service_account_json"]
    )
    credentials = google_sa_oauth2_manager.get_oauth2_credentials()

    client = bigquery.Client(
        project=HANDLER_KWARGS["connection_data"]["project_id"],
        credentials=credentials
    )

    with open("mindsdb/integrations/handlers/bigquery_handler/tests/seed.sql", "r") as f:
        for line in f.readlines():
            query = client.query(line)
            query.result()


def check_valid_response(res):
    """
    Utility function to check if the response is valid.
    """
    if res.resp_type == RESPONSE_TYPE.TABLE:
        assert res.data_frame is not None, "expected to have some data, but got None"
    assert (
        res.error_code == 0
    ), f"expected to have zero error_code, but got {res.error_code}"
    assert (
        res.error_message is None
    ), f"expected to have None in error message, but got {res.error_message}"


def get_table_names(snowflake_handler):
    """
    Utility function to get the table names from the Snowflake account.
    """
    res = snowflake_handler.get_tables()
    tables = res.data_frame

    assert tables is not None, "expected to have some tables in the db, but got None"
    assert (
        "table_name" in tables
    ), f"expected to get 'table_name' column in the response:\n{tables}"

    return list(tables["table_name"])


@pytest.mark.usefixtures("bigquery_handler")
class TestBigQueryHandlerConnect:
    def test_connect(self, bigquery_handler):
        """
        Tests the `connect` method to ensure it connects to the BigQuery warehouse.
        """
        bigquery_handler.connect()
        assert bigquery_handler.is_connected, "the handler has failed to connect"

    def test_check_connection(self, bigquery_handler):
        """
        Tests the `check_connection` method to verify that it returns a StatusResponse object and accurately reflects the connection status.
        """
        res = bigquery_handler.check_connection()
        assert res.success, res.error_message


@pytest.mark.usefixtures("bigquery_handler")
class TestBigQueryHandlerTables:
    table_for_creation = "TEST_MDB"

    def test_get_tables(self, bigquery_handler):
        """
        Tests the `get_tables` method to confirm it correctly calls `native_query` with the appropriate SQL commands.
        """
        res = bigquery_handler.get_tables()
        assert res.type == RESPONSE_TYPE.TABLE, "expected a TABLE"
        assert len(res.data_frame) > 0, "expected > O columns"

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

    def test_get_columns(self, bigquery_handler):
        """
        Tests if the `get_columns` method correctly constructs the SQL query and if it calls `native_query` with the correct query.
        """
        res = bigquery_handler.get_columns("TEST")
        assert res.type == RESPONSE_TYPE.TABLE, "expected a TABLE"
        assert len(res.data_frame) > 0, "expected > O columns"

        views = res.data_frame

        expected_columns = {
            "Field": ["COL_ONE", "COL_FOUR", "COL_TWO", "COL_THREE"],
            "Type": ["INT64", "STRING", "INT64", "FLOAT64"],
        }
        expected_df = pd.DataFrame(expected_columns)

        # Sort both DataFrames by all columns before comparing
        views = views.sort_values(by=list(res.data_frame.columns)).reset_index(drop=True)
        expected_df = expected_df.sort_values(by=list(expected_df.columns)).reset_index(drop=True)

        assert views.equals(
            expected_df
        ), "response does not contain the expected columns"

    def test_create_table(self, bigquery_handler):
        """
        Tests a table creation query to ensure it creates a table in the Snowflake account.
        """
        query = f"""
            CREATE TABLE IF NOT EXISTS {self.table_for_creation} (
                test_col INT
            );
        """
        res = bigquery_handler.native_query(query)
        check_valid_response(res)

        tables = get_table_names(bigquery_handler)

        assert (
            self.table_for_creation in tables
        ), f"expected to have {self.table_for_creation} in database, but got: {tables}"

    def test_drop_table(self, bigquery_handler):
        """
        Tests a table drop query to ensure it drops a table in the Snowflake account.
        """
        query = f"DROP TABLE IF EXISTS {self.table_for_creation}"
        res = bigquery_handler.native_query(query)
        check_valid_response(res)

        tables = get_table_names(bigquery_handler)

        assert self.table_for_creation not in tables


@pytest.mark.usefixtures("bigquery_handler")
class TestBigQueryHandlerQuery:
    def test_select_native_query(self, bigquery_handler):
        """
        Tests the `native_query` method to ensure it executes a SQL query using a mock cursor and returns a Response object.
        """
        query = "SELECT * FROM TEST"
        res = bigquery_handler.native_query(query)

        assert type(res) is Response
        assert res.resp_type == RESPONSE_TYPE.TABLE

        expected_data = {
            "COL_ONE": [1, 2, 3],
            "COL_TWO": [-1, -2, -3],
            "COL_THREE": [0.1, 0.2, 0.3],
            "COL_FOUR": ["A", "B", "C"],
        }
        expected_df = pd.DataFrame(expected_data)

        assert res.data_frame.equals(
            expected_df
        ), "response does not contain the expected data"

    def test_select_query(self, bigquery_handler):
        """
        Tests the `query` method to ensure it executes a SQL query and returns a Response object.
        """
        limit = 3
        query = "SELECT * FROM TEST"
        res = bigquery_handler.query(query)
        check_valid_response(res)

        got_rows = res.data_frame.shape[0]
        want_rows = limit

        assert (
            got_rows == want_rows
        ), f"expected to have {want_rows} rows in response but got: {got_rows}"


if __name__ == "__main__":
    pytest.main([__file__])

import os
import pytest
import pandas as pd
from google.cloud import bigquery

from mindsdb.integrations.handlers.bigquery_handler.bigquery_handler import BigQueryHandler
from mindsdb.integrations.utilities.handlers.auth_utilities import GoogleServiceAccountOAuth2Manager
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


if __name__ == "__main__":
    pytest.main([__file__])
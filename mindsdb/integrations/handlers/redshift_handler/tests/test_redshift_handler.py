import os
import pytest
import psycopg

from mindsdb.integrations.handlers.postgres_handler.tests.test_postgres_handler import (
    TestPostgresConnection,
    TestPostgresQuery,
    TestPostgresTables,
    TestPostgresColumns,
    TestPostgresDisconnect
)

from mindsdb.integrations.handlers.redshift_handler.redshift_handler import RedshiftHandler


HANDLER_KWARGS = {
    "connection_data": {
        "host": os.environ.get("MDB_TEST_REDSHIFT_HOST", "127.0.0.1"),
        "port": os.environ.get("MDB_TEST_REDSHIFT_PORT", "5439"),
        "user": os.environ.get("MDB_TEST_REDSHIFT_USER", "redshift"),
        "password": os.environ.get("MDB_TEST_REDSHIFT_PASSWORD", "supersecret"),
        "database": os.environ.get("MDB_TEST_REDSHIFT_DATABASE", "redshift_db_handler_test").lower()
    }
}


@pytest.fixture(scope="module")
def handler():
    seed_db()
    handler = RedshiftHandler("test_redshift_handler", **HANDLER_KWARGS)
    yield handler
    handler.disconnect()


def seed_db():
    """
    Seed the test DB by running the queries in the seed.sql file.
    """
    # Connect to the dev (default) database to run seed queries
    conn_info = HANDLER_KWARGS["connection_data"].copy()
    del conn_info["database"]
    conn_info["dbname"] = "dev"
    db = psycopg.connect(**conn_info)
    db.autocommit = True

    cursor = db.cursor()

    # Check if the database exists
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{HANDLER_KWARGS['connection_data']['database']}'")
    result = cursor.fetchone()
    # If the database exists, drop it
    if result:
        cursor.execute(f"DROP DATABASE {HANDLER_KWARGS['connection_data']['database']}")
    # Create the test database
    cursor.execute(f"CREATE DATABASE {HANDLER_KWARGS['connection_data']['database']}")

    # Reconnect to the new database
    conn_info["dbname"] = HANDLER_KWARGS['connection_data']['database']
    db = psycopg.connect(**conn_info)
    db.autocommit = True
    cursor = db.cursor()

    with open("mindsdb/integrations/handlers/redshift_handler/tests/seed.sql", "r") as f:
        for line in f.readlines():
            cursor.execute(line)
    cursor.close()
    db.close()


@pytest.mark.redshift
class TestRedshiftHandlerConnection(TestPostgresConnection):
    def test_connect(self, handler):
        """
        Tests the `connect` method to ensure it connects to the Redshift warehouse and sets the `is_connected` flag to True.
        """
        super().test_connect(handler)

    def test_check_connection(self, handler):
        """
        Tests the `check_connection` method to verify that it returns a StatusResponse object and accurately reflects the connection status.
        """
        super().test_check_connection(handler)


@pytest.mark.redshift
class TestRedshiftHandlerQuery(TestPostgresQuery):
    def test_native_query_show_dbs(self, handler):
        """
        Tests the `native_query` method to ensure it returns a list of databases in the Redshift warehouse.
        """
        dbs = handler.native_query("SELECT datname FROM pg_database")
        dbs = dbs.data_frame
        assert dbs is not None, "expected to get some data, but got None"
        assert "datname" in dbs, f"Expected to get 'datname' column in response:\n{dbs}"
        dbs = list(dbs["datname"])
        expected_db = HANDLER_KWARGS["connection_data"]["database"]
        assert (
            expected_db in dbs
        ), f"expected to have {expected_db} db in response: {dbs}"

    def test_select_query(self, handler):
        """
        Tests the `query` method with SELECT query to ensure it returns the expected data from the Redshift warehouse.
        """
        super().test_select_query(handler)


@pytest.mark.redshift
class TestRedshiftHandlerTables(TestPostgresTables):
    def test_get_tables(self, handler):
        """
        Tests the `get_tables` method to confirm it returns a list of tables in the Redshift warehouse.
        """
        super().test_get_tables(handler)

    def test_create_table(self, handler):
        """
        Tests a table creation query to ensure it creates a table in the Redshift warehouse.
        """
        super().test_create_table(handler)

    def test_drop_table(self, handler):
        """
        Tests a table drop query to ensure it drops a table in the Redshift warehouse.
        """
        super().test_drop_table(handler)


@pytest.mark.redshift
class TestRedshiftHandlerColumns(TestPostgresColumns):
    def test_get_columns(self, handler):
        """
        Tests the `get_columns` method to confirm it returns a list of columns of the test table created in the Redshift warehouse.
        """
        super().test_get_columns(handler)


@pytest.mark.redshift
class TestRedshiftHandlerDisconnect(TestPostgresDisconnect):
    def test_disconnect(self, handler):
        """
        Tests the `disconnect` method to ensure it disconnects from the Redshift warehouse and sets the `is_connected` flag to False.
        """
        super().test_disconnect(handler)

    def test_check_connection(self, handler):
        """
        Tests the `check_connection` method to verify that it returns a StatusResponse object and accurately reflects the connection status.
        """
        super().test_check_connection(handler)


if __name__ == "__main__":
    pytest.main(["-m", "redshift", __file__])

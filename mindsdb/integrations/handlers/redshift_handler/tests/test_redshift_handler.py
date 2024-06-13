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
class TestRedshiftConnection(TestPostgresConnection):
    def test_connect(self, handler):
        super().test_connect(handler)

    def test_check_connection(self, handler):
        super().test_check_connection(handler)


if __name__ == "__main__":
    pytest.main(["-m", "redshift", __file__])
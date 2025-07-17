import pytest
import json
from pymongo import MongoClient

from mindsdb_sql_parser.ast import Identifier, Select, Star
from mindsdb.integrations.handlers.mongodb_handler.mongodb_handler import MongoDBHandler
from mindsdb.integrations.libs.response import RESPONSE_TYPE


HANDLER_KWARGS = {
    "connection_data": {
        "host": "127.0.0.1",
        "port": "27017",
        "username": "test_user",
        "password": "supersecret",
        "database": "mongo_test_db",
    }
}

expected_columns = ["_id", "col_one", "col_two", "col_three", "col_four"]


def seed_db():
    """Seed the test DB with some data"""
    creds = HANDLER_KWARGS["connection_data"]
    uri = f"mongodb://{creds['username']}:{creds['password']}@{creds['host']}"
    conn = MongoClient(uri)
    db = conn[HANDLER_KWARGS["connection_data"]["database"]]  # noqa

    with open("mindsdb/integrations/handlers/mongodb_handler/tests/seed.json", "r") as f:
        json.load(f)
    conn.close()


@pytest.fixture(scope="module")
def handler(request):
    seed_db()
    handler = MongoDBHandler("mongo_handler", **HANDLER_KWARGS)
    return handler


def check_valid_response(res):
    if res.resp_type == RESPONSE_TYPE.TABLE:
        assert res.data_frame is not None, "expected to have some data, but got None"
    assert (
        res.error_code == 0
    ), f"expected to have zero error_code, but got {res.error_code}"
    assert (
        res.error_message is None
    ), f"expected to have None in error message, but got {res.error_message}"


""" TESTS """

# TODO - Subscribe


class TestMongoDBConnection:
    def test_connect(self, handler):
        handler.connect()
        assert handler.is_connected, "connection error"

    def test_check_connection(self, handler):
        res = handler.check_connection()
        assert res.success is True, res.error_message


# TODO - Subscribe


class TestMongoDBQuery:
    def test_native_query(self, handler):
        query_string = "db.test.find()"
        response = handler.native_query(query_string)
        dbs = response.data_frame
        assert dbs is not None, "expected to get some data, but got None"
        assert "col_one" in dbs, f"expected to get 'col_one' column in response:\n{dbs}"

    def test_select_query(self, handler):
        limit = 3
        query = Select(
            targets=[Star()],
            from_table=Identifier(parts=["test"]),
        )
        res = handler.query(query)
        check_valid_response(res)
        got_rows = res.data_frame.shape[0]
        want_rows = limit
        assert (
            got_rows == want_rows
        ), f"expected to have {want_rows} rows in response but got: {got_rows}"


class TestMongoDBTables:
    def test_get_tables(self, handler):
        res = handler.get_tables()
        tables = res.data_frame
        test_table = list(tables["table_name"])
        assert (
            tables is not None
        ), "expected to have some table_name in the db, but got None"
        assert (
            "table_name" in tables
        ), f"expected to get 'table_name' column in the response:\n{tables}"
        # get a specific table from the tables list
        assert (
            "test" in test_table
        ), f"expected to have 'test' table in the db but got: {test_table}"


class TestMongoDBColumns:
    def test_get_columns(self, handler):
        described = handler.get_columns("test")
        describe_data = described.data_frame
        check_valid_response(described)
        got_columns = list(describe_data.iloc[:, 0])
        assert (
            got_columns == expected_columns
        ), f"expected to have next columns in test table:\n{expected_columns}\nbut got:\n{got_columns}"


class TestMongoDBDisconnect:
    def test_disconnect(self, handler):
        handler.disconnect()
        assert handler.is_connected is False, "failed to disconnect"

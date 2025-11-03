import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from bson import ObjectId
from mindsdb_sql_parser import ast
from mindsdb_sql_parser.ast.select.star import Star
import pymongo
from pymongo.errors import InvalidURI, OperationFailure
import pymongo.results

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.mongodb_handler.mongodb_handler import MongoDBHandler


class TestMongoDBHandler(BaseHandlerTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(host="mongodb://localhost:27017", database="sample_mflix")

    def create_handler(self):
        return MongoDBHandler("mongodb", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.mongodb_handler.mongodb_handler.MongoClient")

    def test_connect_success(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that pymongo.MongoClient is called exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()
        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_connect_failure_with_invalid_uri(self):
        """
        Test if `connect` method raises InvalidURI exception when an invalid URI is provided.
        """
        self.mock_connect.side_effect = InvalidURI

        with self.assertRaises(InvalidURI):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

    def test_connect_failure_with_incorrect_credentials(self):
        """
        Test if `connect` method raises OperationFailure exception when incorrect credentials are provided.
        """
        self.mock_connect.side_effect = OperationFailure(error="Authentication failed.")

        with self.assertRaises(OperationFailure):
            self.handler.connect()

        self.assertFalse(self.handler.is_connected)

    def test_check_connection_failure_with_non_existent_database(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on failed connection due to non-existent database.
        """
        self.mock_connect.return_value.list_database_names.return_value = ["demo"]

        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_check_connection_success(self):
        """
        Test if the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        self.mock_connect.return_value.list_database_names.return_value = ["sample_mflix"]

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_query_failure_with_non_existent_collection(self):
        """
        Test if the `query` method returns a response object with an error message on failed query due to non-existent collection.
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("theaters"),
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertTrue(response.error_message)

    def test_query_failure_with_unsupported_query_type(self):
        """
        Test if the `query` method raises NotImplementedError on unsupported query operation.
        This exception will be raised in the `to_mongo_query` method of the `MongodbRender` class.
        """
        query = ast.Insert(
            table=ast.Identifier("table1"),
            columns=["id", "name"],
            values=[[1, "Alice"]],
        )

        with self.assertRaises(NotImplementedError):
            self.handler.query(query)

    def test_query_failure_with_unsupported_operation(self):
        """
        Test if the `query` method raises NotImplementedError on unsupported operation.
        This exception will be raised in the `handle_where` method of the `MongodbRender` class.
        """
        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("movies"),
            where=ast.BinaryOperation(args=[ast.Identifier("name"), ast.Constant("The Dark Knight")], op="in"),
        )

        with self.assertRaises(NotImplementedError):
            self.handler.query(query)

    def test_query_select_success(self):
        """
        Test if the `query` method returns a response object with a data frame containing the query result.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]]["movies"].aggregate.return_value = [
            {
                "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
                "name": "The Dark Knight",
                "plot": "The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan.",
                "runtime": 152,
            }
        ]

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("movies"),
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["_id", "name", "plot", "runtime"])
        self.assertEqual(df["name"].tolist(), ["The Dark Knight"])

    def test_query_update_success(self):
        """
        Test if the `query` method returns a response object with a 'OK' status.
        `native_query` cannot be tested directly because it depends on some pre-processing steps handled by the `query` method.
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]][
            "movies"
        ].update_many.return_value = pymongo.results.UpdateResult(
            acknowledged=True, raw_result={"n": 1, "nModified": 1}
        )

        query = ast.Update(
            table=ast.Identifier("movies"),
            update_columns={
                "name": ast.Constant("The Dark Knight"),
                "plot": ast.Constant(
                    "The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan."
                ),
                "runtime": ast.Constant(152),
            },
            where=ast.BinaryOperation(args=[ast.Identifier("name"), ast.Constant("The Dark Knight")], op="="),
        )

        response = self.handler.query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.OK)

    def test_get_tables(self):
        """
        Tests the `get_tables` method returns a response object with a list of tables (collections) in the database.
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "theaters",
            "movies",
            "comments",
            "sessions",
            "users",
            "embedded_movies",
        ]

        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 6)
        self.assertEqual(df.columns.tolist(), ["table_name"])
        self.assertEqual(
            df["table_name"].tolist(),
            ["theaters", "movies", "comments", "sessions", "users", "embedded_movies"],
        )

    def test_get_columns(self):
        """
        Tests the `get_columns` method returns a response object with a list of columns (fields) for a given table (collection).
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]]["movies"].find_one.return_value = {
            "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
            "name": "The Dark Knight",
            "plot": "The Dark Knight is a 2008 superhero film directed, produced, and co-written by Christopher Nolan.",
            "runtime": 152,
        }

        response = self.handler.get_columns("movies")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 4)
        self.assertEqual(df.columns.tolist(), ["Field", "Type"])
        self.assertEqual(df["Field"].tolist(), ["_id", "name", "plot", "runtime"])
        self.assertEqual(df["Type"].tolist(), ["str", "str", "str", "int"])

    # use subquery for select
    def test_query_select_with_subquery_success(self):
        """
        Test if the `query` method returns a response object with a data frame containing the query result for a select with subquery.
        e.g., SELECT * FROM (SELECT * FROM theaters);
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies",
            "theaters",
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]]["theaters"].aggregate.return_value = [
            {
                "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
                "name": "Cinema City",
                "location": "Downtown",
            }
        ]

        subquery = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("theaters"),
        )

        main_query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=subquery,
        )

        response = self.handler.query(main_query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["_id", "name", "location"])
        self.assertEqual(df["name"].tolist(), ["Cinema City"])

    def test_query_select_with_complex_subquery_success(self):
        """
        Test if the `query` method returns a response object with a data frame containing the query result for a select with complex subquery.
        e.g. SELECT * FROM (SELECT CAST(customer_id AS VARCHAR) AS cust_id, CAST(first_name AS VARCHAR) AS fname FROM mongo_db.customers)
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "customers"
        ]
        self.mock_connect.return_value[self.dummy_connection_data["database"]]["customers"].aggregate.return_value = [
            {"cust_id": "C001", "fname": "John"}
        ]

        cust_cast = ast.TypeCast(
            arg=ast.Identifier(parts=["customer_id"]),
            type_name="VARCHAR",
            precision=None,
        )
        cust_cast.alias = ast.Identifier(parts=["cust_id"])

        fname_cast = ast.TypeCast(
            arg=ast.Identifier(parts=["first_name"]),
            type_name="VARCHAR",
            precision=None,
        )
        fname_cast.alias = ast.Identifier(parts=["fname"])

        subquery = ast.Select(
            targets=[cust_cast, fname_cast],
            from_table=ast.Identifier(parts=["mongo_db", "customers"]),
            where=None,
            group_by=None,
            having=None,
            order_by=None,
            limit=None,
            offset=None,
            distinct=False,
            modifiers=None,
            cte=None,
            mode=None,
        )

        main_query = ast.Select(
            targets=[ast.Star()],
            from_table=subquery,
            where=None,
            group_by=None,
            having=None,
            order_by=None,
            limit=ast.Constant(50),
            offset=None,
            distinct=False,
            modifiers=None,
            cte=None,
            mode=None,
        )

        response = self.handler.query(main_query)

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["cust_id", "fname"])
        self.assertEqual(df["cust_id"].tolist(), ["C001"])
        self.assertEqual(df["fname"].tolist(), ["John"])

    def test_query_select_with_where_operators(self):
        """
        Test SELECT with various WHERE operators (>, <, >=, <=, !=)
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]]["movies"].aggregate.return_value = [
            {
                "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
                "name": "Inception",
                "runtime": 148,
            }
        ]

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("movies"),
            where=ast.BinaryOperation(args=[ast.Identifier("runtime"), ast.Constant(150)], op="<"),
        )

        response = self.handler.query(query)

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["_id", "name", "runtime"])
        self.assertEqual(df["name"].tolist(), ["Inception"])

    def test_query_select_with_and_or_conditions(self):
        """
        Test SELECT with AND/OR conditions in WHERE clause
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]]["movies"].aggregate.return_value = [
            {
                "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
                "name": "The Matrix",
                "runtime": 136,
            }
        ]

        query = ast.Select(
            targets=[
                Star(),
            ],
            from_table=ast.Identifier("movies"),
            where=ast.BinaryOperation(
                args=[
                    ast.BinaryOperation(args=[ast.Identifier("runtime"), ast.Constant(140)], op="<"),
                    ast.BinaryOperation(
                        args=[ast.Identifier("name"), ast.Constant("The Matrix")],
                        op="=",
                    ),
                ],
                op="AND",
            ),
        )

        response = self.handler.query(query)

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["_id", "name", "runtime"])
        self.assertEqual(df["name"].tolist(), ["The Matrix"])

    def test_unsupported_select_query_(self):
        """
        NotImplementedError for unsupported inner subselect:
        SELECT * FROM (SELECT COUNT(*) FROM movies);
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        inner = ast.Select(
            targets=[
                ast.Function(op="COUNT", args=[ast.Star()], distinct=False, from_arg=None),
            ],
            from_table=ast.Identifier(parts=["movies"]),
        )

        outer = ast.Select(
            targets=[ast.Star()],
            from_table=inner,
        )

        with self.assertRaises(NotImplementedError) as ctx:
            self.handler.query(outer)

        self.assertIn("Unsupported inner target", str(ctx.exception))

    def test_select_with_match_and_projection(self):
        """
        Test SELECT with WHERE clause and specific projections
         if match:
            arg.append({"$match": match})
        if match is not None and proj != {}:
            arg.append({"$project": proj})
        """
        self.mock_connect.return_value[self.dummy_connection_data["database"]].list_collection_names.return_value = [
            "movies"
        ]

        self.mock_connect.return_value[self.dummy_connection_data["database"]]["movies"].aggregate.return_value = [
            {
                "_id": ObjectId("5f5b3f3b3f3b3f3b3f3b3f3b"),
                "name": "Interstellar",
            }
        ]

        query = ast.Select(
            targets=[
                ast.Identifier("name"),
            ],
            from_table=ast.Identifier("movies"),
            where=ast.BinaryOperation(args=[ast.Identifier("runtime"), ast.Constant(170)], op=">"),
        )

        response = self.handler.query(query)

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 1)
        self.assertEqual(df.columns.tolist(), ["_id", "name"])
        self.assertEqual(df["name"].tolist(), ["Interstellar"])


if __name__ == "__main__":
    unittest.main()

import importlib
from unittest.mock import patch

import pytest
import pandas as pd
from mindsdb_sql import parse_sql

from unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("qdrant")
    QDRANT_INSTALLED = True
except ImportError:
    QDRANT_INSTALLED = False


# @pytest.mark.skipif(not QDRANT_INSTALLED, reason="qdrant handler is not installed")
class TestQdrantHandler(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))

        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):
        super().setup_method()
        # connect to a Qdrant instance and create a database
        self.run_sql(
            """
            CREATE DATABASE qtest
            WITH ENGINE = "qdrant",
            PARAMETERS = {
                "location": ":memory:",
                "collection_config": {
                    "size": 3,
                    "distance": "Cosine"
                }
            }
            """
        )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_with_select(self, postgres_handler_mock):
        df = pd.DataFrame(
            {

                "id": [1, 2],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )

        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        sql = """
        CREATE TABLE qtest.test_table_1 (
            SELECT * FROM pg.df
        )
        """
        self.run_sql(sql)

    @pytest.mark.xfail(reason="drop table for vectordatabase is not working")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_drop_table(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [32, 13],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE qtest.test_table_2 (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        sql = """
            DROP TABLE qtest.test_table_2;
        """
        self.run_sql(sql)

        sql = """
            DROP TABLE qtest.test_table_22;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_insert_into(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [81, 24, 33],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        df2 = pd.DataFrame(
            {
                "id": [1, 2, 3],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 3.0, 4.0],
                    [1.0, 2.0],
                    [1.0, 2.0, 3.0],
                ],  # different dimensions
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"df": df, "df2": df2})
        num_record = df.shape[0]

        # create a table
        sql = """
            CREATE TABLE qtest.test_table_3 (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # insert into a table with values
        sql = """
            INSERT INTO qtest.test_table_3 (
                id,content,metadata,embeddings
            )
            VALUES (
                4, 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
       # check if the data is inserted
        sql = """
            SELECT * FROM qtest.test_table_3
            WHERE id = 4 AND search_vector = '[1.0, 2.0, 3.0]' AND search_vector= '[1.0, 2.0, 4.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # insert without ids should auto-generate ids
        sql = """
            INSERT INTO qtest.test_table_3 (
                content,metadata,embeddings
            )
            VALUES (
                'this is a test without ID', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
            """
        self.run_sql(sql)

        # check if the data is inserted
        sql = """
            SELECT * FROM qtest.test_table_3
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record + 2

        # insert into a table with a select statement
        sql = """
            INSERT INTO qtest.test_table_3 (
                content,metadata,embeddings
            )
            SELECT
                content,metadata,embeddings
            FROM
                pg.df
        """
        self.run_sql(sql)

        # check if the data is inserted
        sql = """
            SELECT * FROM qtest.test_table_3
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record * 2 + 2

        # insert into a table with a select statement, but wrong columns
        sql = """
                INSERT INTO qtest.test_table_3
                SELECT
                    content,metadata,embeddings as wrong_column
                FROM
                    pg.df
            """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # insert into a table with a select statement, missing metadata column
        sql = """
            INSERT INTO qtest.test_table_3
            SELECT
                content,embeddings
            FROM
                pg.df
        """
        self.run_sql(sql)

        # insert into a table with a select statement, missing embedding column, shall raise an error
        sql = """
                INSERT INTO qtest.test_table_3
                SELECT
                    content,metadata
                FROM
                    pg.df
            """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # insert into a table with a select statement, with different embedding dimensions, shall raise an error
        sql = """
            INSERT INTO qtest.test_table_3
            SELECT
                content,metadata,embeddings
            FROM
                pg.df2
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

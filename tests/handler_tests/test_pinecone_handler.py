# check if chroma_db is installed
import importlib
from unittest.mock import patch
import os

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest

try:
    pinecone = importlib.import_module("pinecone")
    PINECONE_CLIENT_INSTALLED = True
except ImportError:
    PINECONE_CLIENT_INSTALLED = False


# NOTE: These tests might fail since pinecone is eventually consistent. Some queries return wrong result when tested

@pytest.mark.skipif(not PINECONE_CLIENT_INSTALLED, reason="pinecone client is not installed")
class TestPineconeHandler(BaseExecutorTest):

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
        # Replace with your pinecone key
        self.api_key = os.environ['PINECONE_API_KEY']
        self.environment = os.environ['PINECONE_ENV']
        self.run_sql(f"""
            CREATE DATABASE pinecone_test
            WITH ENGINE = "pinecone",
            PARAMETERS = {{
               "api_key": "{self.api_key}",
               "environment": "{self.environment}"
            }}
        """)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_with_select(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], [1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 3.0]],
            }
        )

        self.set_handler(postgres_handler_mock, "pg", tables={"df": df})
        sql = """
        CREATE TABLE pinecone_test.testtable (
            SELECT * FROM pg.df
        )
        """
        self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_select_from(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], [1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 3.0]],
            }
        )

        self.set_handler(postgres_handler_mock, "pg", tables={"testtable": df})
        sql = """
        CREATE TABLE pinecone_test.testtable (
            SELECT * FROM pg.df
        )
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM pinecone_test.testtable
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with out limit
        sql = """
            SELECT * FROM pinecone_test.testtable
            WHERE search_vector = '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM pinecone_test.testtable
            WHERE search_vector = '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM pinecone_test.testtable
            WHERE testable.metadata.test = 'test'
            AND search_vector = '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_insert_into(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], [1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 3.0]],
            }
        )

        df2 = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 3.0, 4.0, 3.0, 5.0, 2.0, 8.1],
                    [4.0, 2.0, 7.0, 4.0, 2.0, 5.0, 2.0, 9.0],
                    [5.0, 2.0, 3.0, 2.0, 3.0, 3.0, 2.0, 7.0],
                ],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"df": df, "df2": df2})

        sql = """
            CREATE TABLE pinecone_test.testtable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        sql = """
            INSERT INTO pinecone_test.testtable (
                id,content,metadata,embeddings
            )
            VALUES (
                'some_unique_id', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM pinecone_test.testtable
            WHERE id = 'some_unique_id'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 3

        # insert into a table with existing id, shall work
        sql = """
            INSERT INTO pinecone_test.testtable (
                id,content,metadata,embeddings
            )
            VALUES (
                'id1', 'this is a test', '{"test": "tester"}', '[1.0, 2.0, 3.0, 4.0, 6.0, 7.0, 8.0, 9.0]'
            )
        """
        self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_delete(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}],
                "embeddings": [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], [1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testtable": df})

        # create a table
        sql = """
            CREATE TABLE pinecone_test.testtable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # delete from a table with a metadata filter
        sql = """
            DELETE FROM pinecone_test.testtable
            WHERE testtable.metadata.test = 'test1'
        """
        self.run_sql(sql)

        # delete by id
        sql = """
            DELETE FROM pinecone_test.testtable
            WHERE id = 'id2'
        """
        self.run_sql(sql)

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM pinecone_test.testtable
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @pytest.mark.xfail(reason="update for pinecone is not implemented, use insert")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE pinecone_test.testtable
            SET metadata.test = 'test2'
            WHERE metadata.test = 'test'
        """
        # sql shoudl fail
        with pytest.raises(Exception):
            self.run_sql(sql)

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_table(self, postgres_handler_mock):
        # create an empty table
        sql = """
            CREATE TABLE pinecone_test.testtable;
        """
        self.run_sql(sql)
        # create a table with the schema definition is not allowed
        sql = """
            CREATE TABLE pinecone_test.testtable (
                id int,
                metadata text,
                embedding float[]
            );
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @pytest.mark.xfail(reason="drop table for vectordatabase is not working")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_drop_table(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], [1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 9.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testtable": df})
        sql = """
            CREATE TABLE piecone_test.testtable(
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)
        sql = """
            DROP TABLE pinecone_test.testtable;
        """
        self.run_sql(sql)
        sql = """
            DROP TABLE pinecone_test.testtable2;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

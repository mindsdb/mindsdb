# check if chroma_db is installed
import importlib
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("chromadb")
    CHROMA_DB_INSTALLED = True
except ImportError:
    CHROMA_DB_INSTALLED = False


@pytest.mark.skipif(not CHROMA_DB_INSTALLED, reason="chroma_db is not installed")
class TestChromaDBHandler(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))

        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setUp(self):
        # create the ml engine
        self.run_sql(
            """
            CREATE ML_ENGINE my_chroma_db
            FROM chromadb
        """
        )
        # create a chroma database
        self.run_sql(
            """
            CREATE DATABASE chroma_test
            WITH ENGINE = "my_chroma_db"
        """
        )

        # create a table
        self.run_sql(
            """
            CREATE TABLE chroma_test.test_table
            """
        )

        # insert some data
        self.run_sql(
            """
            INSERT INTO chroma_test.test_table (
                id,
                content,
                metadata,
                embedding
            )
            VALUES (
                1,
                'this is a test',
                '{"test": "test"}',
                [1.0, 2.0, 3.0]
            ), (
                2,
                'this is another test',
                '{"test": "test"}',
                [1.0, 2.0, 3.0]
            )
        """
        )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_table(self, postgres_handler_mock):
        # create an empty table
        sql = """
            CREATE TABLE chroma_test.test_table;
        """
        self.run_sql(sql)

        # create a table with the schema definition is not allowed
        sql = """
            CREATE TABLE chroma_test.test_table (
                id int,
                metadata text,
                embedding float[]
            );
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # create a table with a select statement is not allowed
        sql = """
            CREATE TABLE chroma_test.test_table (
                SELECT * FROM chroma_test.test_table
            )
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_drop_table(self):
        # create a table
        sql = """
            CREATE TABLE chroma_test.test_table2;
        """
        self.run_sql(sql)

        # drop a table
        sql = """
            DROP TABLE chroma_test.test_table2;
        """
        self.run_sql(sql)

        # drop a non existent table will raise an error
        sql = """
            DROP TABLE chroma_test.test_table2;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_insert_into(self):
        # insert into a table with values
        sql = """
            INSERT INTO chroma_test.test_table (
                id,content,metadata,embedding
            )
            VALUES (
                'some_unique_id', 'this is a test', '{"test": "test"}', [1.0, 2.0, 3.0]
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE id = 'some_unique_id'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # insert without specifying id should also work
        sql = """
            INSERT INTO chroma_test.test_table (
                content,metadata,embedding
            )
            VALUES (
                'this is a test', '{"test": "test"}', [1.0, 2.0, 3.0]
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM chroma_test.test_table
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # insert into a table with a select statement
        sql = """
            INSERT INTO chroma_test.test_table (
                content,metadata,embedding
            )
            SELECT
                content,metadata,embedding
            FROM chroma_test.test_table
            WHERE id = 'some_unique_id'
        """

        # insert into a table with a select statement, but wrong columns

        # insert into a table with a select statement, missing id column

        # insert into a table with a select statement, missing metadata column

        # insert into a table with a select statement, missing embedding column, shall raise an error

        # insert into a table with a select statement, with different embedding dimensions, shall raise an error

        # upsert into a table with a select statement
        ...

    def test_select_from(self):
        # query a table without any filters
        sql = """
            SELECT * FROM chroma_test.test_table
        """
        self.run_sql(sql)

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE search_embedding = [1.0, 2.0, 3.0]
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE search_vector = [1.0, 2.0, 3.0]
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test'
            AND search_vector = [1.0, 2.0, 3.0]
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test2'
            WHERE metadata.test = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update the embeddings
        sql = """
            UPDATE chroma_test.test_table
            SET embedding = [3.0, 2.0, 1.0]
            WHERE metadata.test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]

        # update multiple columns
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test3',
                embedding = [1.0, 2.0, 3.0]
                content = 'this is a test'
            WHERE metadata.test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table without any filters is allowed
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test3'
            WHERE metadata.test = 'test2'
            AND search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_delete(self):
        # delete from a table with a metadata filter
        sql = """
            DELETE FROM chroma_test.test_table
            WHERE metadata.test = 'test'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE metadata.test = 'test'
        """

        # delete from a table with a search vector filter is not allowed
        sql = """
            DELETE FROM chroma_test.test_table
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM chroma_test.test_table
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

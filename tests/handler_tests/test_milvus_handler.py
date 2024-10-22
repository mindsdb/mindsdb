import importlib
from unittest.mock import patch

import pandas as pd
import pytest
import time
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    pymilvus = importlib.import_module("pymilvus")
    MILVUS_INSTALLED = True
except ImportError:
    MILVUS_INSTALLED = False


@pytest.mark.skipif(not MILVUS_INSTALLED, reason="pymilvus is not installed")
class TestMilvusHandler(BaseExecutorTest):

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            return ret.data.to_df()

    def setup_method(self):
        super().setup_method()
        # create a milvus database
        self.run_sql("""
            CREATE DATABASE milvus_test
            WITH
              ENGINE = 'milvus',
              PARAMETERS = {
                "uri": "./milvus.db",
                "create_embedding_dim": 3
            };
        """)
        self.run_sql("""
            CREATE DATABASE milvus_test_auto_id
            WITH
              ENGINE = 'milvus',
              PARAMETERS = {
                "uri": "./milvus.db",
                "create_embedding_dim": 3,
                "create_auto_id": true
            };
        """)

    def drop_table(self, table_name):
        pymilvus.connections.connect(
            uri="./milvus.db",
        )
        pymilvus.utility.drop_collection(table_name)

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_table(self, postgres_handler_mock):
        # create an empty table
        sql = """
            CREATE TABLE milvus_test.testable;
        """
        self.run_sql(sql)
        # create a table with the schema definition is not allowed
        sql = """
            CREATE TABLE milvus_test.testable (
                id int,
                metadata text,
                embedding float[]
            );
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_with_select(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )

        self.set_handler(postgres_handler_mock, "pg", tables={"df": df})

        self.drop_table("testable")

        sql = """
        CREATE TABLE milvus_test.testable (
            SELECT * FROM pg.df
        )
        """
        # this should work
        self.run_sql(sql)

        sql = """
        CREATE TABLE milvus_test.testable (
            SELECT * FROM pg.df
        )
        """
        # this should work
        self.run_sql(sql)

        self.drop_table("testable")

    @pytest.mark.xfail(reason="drop table for vectordatabase is not working")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_drop_table(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testable": df})

        self.drop_table("testable")

        # create a table
        sql = """
            CREATE TABLE milvus_test.testable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # drop a table
        sql = """
            DROP TABLE milvus_test.testable;
        """
        self.run_sql(sql)

        # drop a non existent table will raise an error
        sql = """
            DROP TABLE milvus_test.test_table2;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_insert_into(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        df2 = pd.DataFrame(
            {
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 4.0],
                    [1.0, 2.0, 5.0],
                    [1.0, 2.0, 3.0],
                ],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"df": df, "df2": df2})

        self.drop_table("testable")
        self.drop_table("testableauto")

        # create tables
        sql = """
            CREATE TABLE milvus_test.testable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)
        sql = """
            CREATE TABLE milvus_test_auto_id.testableauto (
                SELECT * FROM pg.df2
            )
        """
        self.run_sql(sql)

        # insert into a table with values
        sql = """
            INSERT INTO milvus_test.testable (
                id,content,metadata,embeddings
            )
            VALUES (
                "id4", 'this is a test', '{"test": "test"}', '[1.0, 8.0, 9.0]'
            )
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # check if the data is inserted
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE id = "id4"
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # insert without specifying id should work in autoid one
        sql = """
            INSERT INTO milvus_test_auto_id.testableauto (
                content,metadata,embeddings
            )
            VALUES (
                'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # check if the data is inserted
        sql = """
            SELECT * FROM milvus_test_auto_id.testableauto
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 4

        # insert into a table with a select statement
        sql = """
            INSERT INTO milvus_test_auto_id.testableauto (content,metadata,embeddings)
            SELECT * FROM pg.df2
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # check if the data is inserted
        sql = """
            SELECT * FROM milvus_test.testableauto
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 7

        self.drop_table("testable")
        self.drop_table("testableauto")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_select_from(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testable": df})

        self.drop_table("testable")

        # create a table
        sql = """
            CREATE TABLE milvus_test.testable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # query a table without any filters
        sql = """
            SELECT * FROM milvus_test.testable
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE test = 'test'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE test = 'test'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        self.drop_table("testable")

    @pytest.mark.xfail(reason="update for vectordatabase is not implemented")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE milvus_test.testable
            SET test = 'test2'
            WHERE test = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update the embeddings
        sql = """
            UPDATE milvus_test.testable
            SET embedding = [3.0, 2.0, 1.0]
            WHERE test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]

        # update multiple columns
        sql = """
            UPDATE milvus_test.testable
            SET test = 'test3',
                embedding = [1.0, 2.0, 3.0]
                content = 'this is a test'
            WHERE test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE test = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE milvus_test.testable
            SET `metadata.test = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table without any filters is allowed
        sql = """
            UPDATE milvus_test.testable
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM milvus_test.testable
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE milvus_test.testable
            SET metadata.test = 'test3'
            WHERE metadata.test = 'test2'
            AND search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_delete(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )

        self.drop_table("testable")

        self.set_handler(postgres_handler_mock, "pg", tables={"testable": df})

        # create a table
        sql = """
            CREATE TABLE milvus_test.testable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # delete by id
        sql = """
            DELETE FROM milvus_test.testable
            WHERE id IN ('id1')
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # check if the data is deleted
        sql = """
            SELECT * FROM milvus_test.testable
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # delete by multiple ids
        sql = """
            DELETE FROM milvus_test.testable
            WHERE id IN ('id2', 'id3')
        """
        self.run_sql(sql)

        time.sleep(1)  # wait for milvus to load the data asynchronously
        # check if the data is deleted
        sql = """
            SELECT * FROM milvus_test.testable
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM milvus_test.testable
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        self.drop_table("testable")

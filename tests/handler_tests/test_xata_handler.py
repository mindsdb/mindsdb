import os
import importlib
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from mindsdb.tests.unit.executor_test_base import BaseExecutorTest

try:
    xata = importlib.import_module("xata")
    XATA_INSTALLED = True
except ImportError:
    XATA_INSTALLED = False


@pytest.mark.skipif(not XATA_INSTALLED, reason="xata is not installed")
class TestXetaHandler(BaseExecutorTest):

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
        self.api_key = os.environ['XATA_TESTING_API_KEY']
        self.db_url = os.environ['XATA_TESTING_DB_URL']
        self.run_sql(f"""
            CREATE DATABASE xata_test
            WITH
              ENGINE = 'xata',
              PARAMETERS = {{
               "api_key": "{self.api_key}",
               "db_url": "{self.db_url}",
               "dimension": 3
            }};
        """)
        self._client = xata.XataClient(api_key=self.api_key, db_url=self.db_url)

    def drop_table(self, table_name):
        resp = self._client.table().delete(table_name)
        if not resp.is_success():
            print(f"Unable to delete {table_name}: {resp['message']}")

    def get_num_records(self, table_name):
        return self._client.search_and_filter().summarize(table_name, {"columns": [], "summaries": {"total": {"count": "*"}}})["summaries"][0]["total"]

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_table(self, postgres_handler_mock):
        # create an empty table
        sql = """CREATE TABLE xata_test.testingtable;"""
        self.run_sql(sql)
        # create a table with the schema definition is not allowed
        sql = """
            CREATE TABLE xata_test.testingtable (
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
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testingtable": df})
        # create a table
        sql = """
            CREATE TABLE xata_test.testingtable (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)
        # drop a table
        sql = """
            DROP TABLE xata_test.testingtable;
        """
        self.run_sql(sql)

    @pytest.mark.xfail(reason="update for vectordatabase is not implemented")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE xata_test.testingtable
            SET metadata.test = 'test2'
            WHERE metadata.test = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE metadata.test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        # update the embeddings
        sql = """
            UPDATE xata_test.testingtable
            SET embedding = '[3.0, 2.0, 1.0]'
            WHERE metadata.test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE metadata.test = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]
        # update multiple columns
        sql = """
            UPDATE xata_test.testingtable
            SET metadata.test = 'test3',
                embedding = '[1.0, 2.0, 3.0]'
                content = 'this is a test'
            WHERE metadata.test = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE metadata.test = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"
        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE xata_test.testingtable
            SET metadata.test = 'test2'
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        with pytest.raises(Exception):
            self.run_sql(sql)
        # update a table without any filters is allowed
        sql = """
            UPDATE xata_test.testingtable
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE metadata.test = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE xata_test.testingtable
            SET metadata.test = 'test3'
            WHERE metadata.test = 'test2'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_with_select(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": ['{"test": "test"}', '{"test": "test"}'],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testingtable": df})
        sql = """
        CREATE TABLE xata_test.testingtable (SELECT * FROM pg.df)
        """
        self.drop_table("testingtable")
        # this should work
        self.run_sql(sql)
        assert self.get_num_records("testingtable") == 2
        self.drop_table("testingtable")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_insert_into(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": ['{"test": "test1"}', '{"test": "test2"}', '{"test": "test3"}'],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        df2 = pd.DataFrame(
            {
                "id": ["id4", "id5", "id6"],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 3.0],
                    [1.0, 2.0, 4.0],
                    [5.0, 2.0, 3.0],
                ],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"df": df, "df2": df2})
        # create a table
        sql = """
            CREATE TABLE xata_test.testingtable (SELECT * FROM pg.df)
        """
        self.drop_table("testingtable")
        self.run_sql(sql)
        # insert into a table with values
        sql = """
            INSERT INTO xata_test.testingtable (id,content,metadata,embeddings)
            VALUES ('some_unique_id', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]')
        """
        self.run_sql(sql)
        self.get_num_records("testingtable") == 4
        # insert without specifying id should also work
        sql = """
            INSERT INTO xata_test.testingtable (content,metadata,embeddings)
            VALUES ('this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]')
        """
        self.run_sql(sql)
        self.get_num_records("testingtable") == 5
        # insert into a table with a select statement
        sql = """
            INSERT INTO xata_test.testingtable (content,metadata,embeddings)
            SELECT content,metadata,embeddings FROM pg.df2
        """
        self.run_sql(sql)
        self.get_num_records("testingtable") == 8
        # insert into a table with a select statement, but wrong columns
        with pytest.raises(Exception):
            sql = """
                INSERT INTO xata_test.testingtable
                SELECT (content,metadata,embeddings as wrong_column) FROM pg.df
            """
            self.run_sql(sql)
        # insert into a table with a select statement, missing metadata column
        sql = """
            INSERT INTO xata_test.testingtable
            SELECT content,embeddings FROM pg.df
        """
        self.run_sql(sql)
        self.get_num_records("testingtable") == 11
        # insert into a table with a select statement, with different embedding dimensions, shall raise an error
        sql = """
            INSERT INTO xata_test.testingtable
            VALUES ('this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0, 4.0]')
        """
        with pytest.raises(Exception):
            self.run_sql(sql)
        self.drop_table("testingtable")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_general_select_queries(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3", "id4", "id5", "id6"],
                "content": ["test content", "test paragraph", "toast types", "", "tast misspelled", "hello"],
                "metadata": ['{"price": 10}', '{"price": 100}', '{"price": 30}', '{"test": "test1"}', '{"test": "test2"}', '{"test": "test3"}'],
                "embeddings": [[1.0, 2.0, 3.0], [5.0, 2.0, 8.0], [3.0, 6.0, 3.0], [1.0, 2.0, 3.0], [3.0, 1.0, 8.0], [1.0, 3.0, 7.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testingtable": df})
        # create a table
        sql = """
            CREATE TABLE xata_test.testingtable (SELECT * FROM pg.df)
        """
        self.drop_table("testingtable")
        self.run_sql(sql)
        # query a table without any filters
        sql = """
            SELECT * FROM xata_test.testingtable
        """
        assert self.run_sql(sql).shape[0] == 6
        # query a table with limit
        sql = """
            SELECT * FROM xata_test.testingtable
            LIMIT 2
        """
        assert self.run_sql(sql).shape[0] == 2
        # query a table with id
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE id = 'id1'
        """
        assert self.run_sql(sql).shape[0] == 1
        # query a table with a metadata filter
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE testingtable.metadata.test = 'test1'
        """
        assert self.run_sql(sql).shape[0] == 1
        # query a table with a metadata complex filter
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE testingtable.metadata.price > 10 AND testingtable.metadata.price <= 100
        """
        assert self.run_sql(sql).shape[0] == 2
        # query a table with a content filter
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE content = 'test content'
        """
        assert self.run_sql(sql).shape[0] == 1
        # query a table with a content filter
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE content = 'test content'
        """
        assert self.run_sql(sql).shape[0] == 1
        # query a table with like operator
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE content LIKE 'test%'
        """
        assert self.run_sql(sql).shape[0] == 2
        self.drop_table("testingtable")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_vector_search(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3", "id4", "id5", "id6"],
                "content": ["test content", "test paragraph", "toast types", "", "tast misspelled", "hello"],
                "metadata": ['{"price": 10}', '{"price": 100}', '{"price": 30}', '{"test": "test1"}', '{"test": "test2"}', '{"test": "test3"}'],
                "embeddings": [[1.0, 2.0, 3.0], [5.0, 2.0, 8.0], [3.0, 6.0, 3.0], [1.0, 2.0, 3.0], [3.0, 1.0, 8.0], [1.0, 3.0, 7.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testingtable": df})
        # create a table
        sql = """
            CREATE TABLE xata_test.testingtable (
                SELECT * FROM pg.df
            )
        """
        self.drop_table("testingtable")
        self.run_sql(sql)
        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        assert self.run_sql(sql).shape[0] == 6
        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        assert self.run_sql(sql).shape[0] == 1
        # query a table with a search vector, and content column
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            AND content LIKE 'test%'
        """
        assert self.run_sql(sql).shape[0] == 2
        # query a table with a metadata filter and a search vector does not work
        sql = """
            SELECT * FROM xata_test.testingtable
            WHERE metadata.price < 200
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        with pytest.raises(Exception):
            self.run_sql(sql)
        self.drop_table("testingtable")

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_delete(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3", "id4", "id5", "id6"],
                "content": ["test content", "test paragraph", "toast types", "", "tast misspelled", "hello"],
                "metadata": ['{"price": 10}', '{"price": 100}', '{"price": 30}', '{"test": "test1"}', '{"test": "test2"}', '{"test": "test3"}'],
                "embeddings": [[1.0, 2.0, 3.0], [5.0, 2.0, 8.0], [3.0, 6.0, 3.0], [1.0, 2.0, 3.0], [3.0, 1.0, 8.0], [1.0, 3.0, 7.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"testingtable": df})
        # create a table
        sql = """
            CREATE TABLE xata_test.testingtable (
                SELECT * FROM pg.df
            )
        """
        self.drop_table("testingtable")
        self.run_sql(sql)
        # delete by id
        sql = """
            DELETE FROM xata_test.testingtable
            WHERE id = 'id2'
        """
        self.run_sql(sql)
        self.get_num_records("testingtable") == 5
        # delete non existant passes
        sql = """
            DELETE FROM xata_test.testingtable
            WHERE id = 'id9'
        """
        self.run_sql(sql)
        self.drop_table("testingtable")

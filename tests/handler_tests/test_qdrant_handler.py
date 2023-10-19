from unittest.mock import patch

import pytest
import pandas as pd
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest


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

        # insert into a table with existing id overwrites the existing record
        sql = """
            INSERT INTO qtest.test_table_3 (
                id,content,metadata,embeddings
            )
            VALUES (
                4, 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_select_from(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [32, 33],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "Info"}, {"test": "Info"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})
        # create a table
        sql = """
            CREATE TABLE qtest.test_table_4 (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # query a table without any filters
        sql = """
            SELECT * FROM qtest.test_table_4
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM qtest.test_table_4
            WHERE id = 32
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM qtest.test_table_4
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM qtest.test_table_4
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM qtest.test_table_4
            WHERE `metadata.test` = 'Info';
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        sql = """
            SELECT * FROM qtest.test_table_4
            WHERE `metadata.test` = 'Info'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    @pytest.mark.xfail(reason="upsert for vectordatabase is not implemented")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE qtest.test_table_5
            SET `metadata.test` = 'test2'
            WHERE `metadata.test` = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM qtest.test_table_5
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update the embeddings
        sql = """
            UPDATE qtest.test_table_5
            SET embedding = [3.0, 2.0, 1.0]
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM qtest.test_table_5
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]

        # update multiple columns
        sql = """
            UPDATE qtest.test_table_5
            SET `metadata.test` = 'test3',
                embedding = [1.0, 2.0, 3.0]
                content = 'this is a test'
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM qtest.test_table_5
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE qtest.test_table_5
            SET `metadata.test = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table without any filters is allowed
        sql = """
            UPDATE qtest.test_table_5
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM qtest.test_table_5
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE qtest.test_table_5
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
                "id": [1, 2],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE qtest.test_table_6 (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # delete from a table with a metadata filter
        sql = """
            DELETE FROM qtest.test_table_6
            WHERE `metadata.test` = 'test1'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM qtest.test_table_6
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # delete by id
        sql = """
            DELETE FROM qtest.test_table_6
            WHERE id = 2
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM qtest.test_table_6
            WHERE id = 2
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 0

        # delete from a table with a search vector filter is not allowed
        sql = """
            DELETE FROM qtest.test_table_6
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM qtest.test_table_6
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

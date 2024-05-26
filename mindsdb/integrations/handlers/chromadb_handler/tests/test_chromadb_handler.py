# check if chroma_db is installed
import importlib
import shutil
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from tests.unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("chromadb")
    CHROMA_DB_INSTALLED = True
except ImportError:
    CHROMA_DB_INSTALLED = False


@pytest.mark.skipif(not CHROMA_DB_INSTALLED, reason="chroma_db is not installed")
class TestChromaDBHandler(BaseExecutorTest):

    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self):
        super().setup_method()
        # create a chroma database under the tmp directory
        tmp_directory = tempfile.mkdtemp()
        self.run_sql(
            f"""
            CREATE DATABASE chroma_test
            WITH ENGINE = "chromadb",
            PARAMETERS = {{
                "persist_directory" : "{tmp_directory}"
            }}
            """
        )
        yield
        # Teardown code: drop the database and remove the temporary directory
        self.run_sql("DROP DATABASE chroma_test;")
        shutil.rmtree(tmp_directory)

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
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

        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        sql = """
        CREATE TABLE chroma_test.test_table2 (
            SELECT * FROM pg.df
        )
        """
        # this should work
        self.run_sql(sql)

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
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE chroma_test.test_table (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # drop a table
        sql = """
            DROP TABLE chroma_test.test_table;
        """
        self.run_sql(sql)

        # drop a non existent table will raise an error
        sql = """
            DROP TABLE chroma_test.test_table2;
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
                "id": ["id1", "id2", "id3"],
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
            CREATE TABLE chroma_test.test_table (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # insert into a table with values
        sql = """
            INSERT INTO chroma_test.test_table (
                id,content,metadata,embeddings
            )
            VALUES (
                'some_unique_id', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
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
                content,metadata,embeddings
            )
            VALUES (
                'this is a test 0', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM chroma_test.test_table
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record + 2

        # insert into a table with a select statement
        sql = """
            INSERT INTO chroma_test.test_table (
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
            SELECT * FROM chroma_test.test_table
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record + 3  # only one unique record was added

        # insert into a table with a select statement, but wrong columns
        with pytest.raises(Exception):
            sql = """
                INSERT INTO chroma_test.test_table
                SELECT
                    content,metadata,embeddings as wrong_column
                FROM
                    pg.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, missing metadata column
        sql = """
            INSERT INTO chroma_test.test_table
            SELECT
                content,embeddings
            FROM
                pg.df
        """
        self.run_sql(sql)

        # insert into a table with a select statement, missing embedding column, shall raise an error
        with pytest.raises(Exception):
            sql = """
                INSERT INTO chroma_test.test_table
                SELECT
                    content,metadata
                FROM
                    pg.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, with different embedding dimensions, shall raise an error
        sql = """
            INSERT INTO chroma_test.test_table
            SELECT
                content,metadata,embeddings
            FROM
                pg.df2
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # TODO: this behavior is not consistent with chromadb doc
        # tracked in https://github.com/chroma-core/chroma/issues/1062
        # insert into a table with existing id, shall raise an error
        # sql = """
        #     INSERT INTO chroma_test.test_table (
        #         id,content,metadata,embeddings
        #     )
        #     VALUES (
        #         'id1', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
        #     )
        # """
        # with pytest.raises(Exception):
        #     self.run_sql(sql)

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
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})
        # create a table
        sql = """
            CREATE TABLE chroma_test.test_table (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # query a table without any filters
        sql = """
            SELECT * FROM chroma_test.test_table
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE `metadata.test` = 'test'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE `metadata.test` = 'test'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_update(self, postgres_handler_mock):

        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"ext_id": "1"}, {"ext_id": "2"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        # create a table
        sql = """
                    CREATE TABLE chroma_test.test_table (
                        SELECT * FROM pg.df
                    )
                """
        self.run_sql(sql)

        # updating the collection with only embeddings and not content is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET embeddings = '[3.0, 2.0, 1.0]',
             id = 'id1'
        """

        with pytest.raises(Exception):
            self.run_sql(sql)

        # updating the collection with only content and not embeddings is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET content = 'blah blah',
             id = 'id1'
        """

        with pytest.raises(Exception):
            self.run_sql(sql)

        # update multiple columns
        sql = """
            UPDATE chroma_test.test_table
            SET id = 'id1',
                embeddings = '[1.0, 2.0, 3.0]',
                content = 'this is a test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1
        assert ret.embeddings[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a where clause is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET `metadata.test` = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table with all columns
        sql = """
                UPDATE chroma_test.test_table
                SET id = 'id1',
                    embeddings = '[1.0, 2.0, 3.0]',
                    content = 'this is a test',
                    `metadata.ext_id` = '1'
                """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE id = 'id1'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1
        assert ret.embeddings[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"
        assert ret.metadata[0] == {"ext_id": "1"}

        # update a table without providing a id is not allowed
        sql = """
            UPDATE chroma_test.test_table
            SET metadata.test = 'test3'

        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_delete(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": ["id1", "id2"],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "pg", tables={"test_table": df})

        # create a table
        sql = """
            CREATE TABLE chroma_test.test_table (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # delete from a table with a metadata filter
        sql = """
            DELETE FROM chroma_test.test_table
            WHERE `metadata.test` = 'test1'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # delete by id
        sql = """
            DELETE FROM chroma_test.test_table
            WHERE id = 'id2'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM chroma_test.test_table
            WHERE id = 'id2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 0

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

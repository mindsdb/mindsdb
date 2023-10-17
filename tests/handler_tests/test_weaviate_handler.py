# check if weaviate is installed
import re
import importlib
from unittest.mock import patch
import tempfile
import psutil
import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..unit.executor_test_base import BaseExecutorTest

try:
    importlib.import_module("weaviate")
    WEAVIATE_INSTALLED = True
except ImportError:
    WEAVIATE_INSTALLED = False


@pytest.mark.skipif(not WEAVIATE_INSTALLED, reason="weaviate is not installed")
class TestWeaviateHandler(BaseExecutorTest):
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
        # create a weaviate database connection
        tmp_directory = tempfile.mkdtemp()
        self.run_sql(
            f"""
            CREATE DATABASE weaviate_test
            WITH ENGINE = "weaviate",
            PARAMETERS = {{
                "persistence_directory": "{tmp_directory}"
            }}
        """
        )

    @staticmethod
    def teardown_class(cls):
        super().teardown_class(cls)
        for proc in psutil.process_iter():
            # check whether the process name matches (kill orphan processes)
            if re.search("weaviate*", proc.name()):
                proc.kill()

    @pytest.mark.xfail(reason="create table for vectordatabase is not well supported")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_create_table(self, postgres_handler_mock):
        # create an empty table
        sql = """
            CREATE TABLE weaviate_test.test_table;
        """
        self.run_sql(sql)

        # create a table with the schema definition is not allowed
        sql = """
            CREATE TABLE weaviate_test.test_table (
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
                "id": [
                    "6af613b6-569c-5c22-9c37-2ed93f31d3af",
                    "b04965e6-a9bb-591f-8f8a-1adcb2c8dc39",
                ],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )

        self.set_handler(postgres_handler_mock, "weaviate", tables={"test_table2": df})

        sql = """
        CREATE TABLE weaviate_test.test_table2 (
            SELECT * FROM weaviate.df
        )
        """
        # this should work
        self.run_sql(sql)

    @pytest.mark.xfail(reason="drop table for vectordatabase is not working")
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_drop_table(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [
                    "4b166dbe-d99d-5091-abdd-95b83330ed3a",
                    "98123fde-012f-5ff3-8b50-881449dac91a",
                ],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "weaviate", tables={"test_table3": df})

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table3 (
                SELECT * FROM weaviate.df
            )
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # drop a table
        sql = """
            DROP TABLE weaviate_test.test_table3;
        """
        self.run_sql(sql)

        # drop a non existent table will raise an error
        sql = """
            DROP TABLE weaviate_test.test_table4;
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_insert_into(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [
                    "6ed955c6-506a-5343-9be4-2c0afae02eef",
                    "c8691da2-158a-5ed6-8537-0e6f140801f2",
                    "a6c4fc8f-6950-51de-a9ae-2c519c465071",
                ],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        df2 = pd.DataFrame(
            {
                "id": [
                    "a9f96b98-dd44-5216-ab0d-dbfc6b262edf",
                    "e99caacd-6c45-5906-bd9f-b79e62f25963",
                    "e4d80b30-151e-51b5-9f4f-18a3b82718e6",
                ],
                "content": ["this is a test", "this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}, {"test": "test3"}],
                "embeddings": [
                    [1.0, 2.0, 3.0, 4.0],
                    [1.0, 2.0],
                    [1.0, 2.0, 3.0],
                ],  # different dimensions
            }
        )
        self.set_handler(
            postgres_handler_mock, "weaviate", tables={"df": df, "df2": df2}
        )
        num_record = df.shape[0]

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table5 (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # insert into a table with values
        sql = """
            INSERT INTO weaviate_test.test_table5 (
                id,content,metadata,embeddings
            )
            VALUES (
                '0159d6c7-973f-5e7a-a9a0-d195d0ea6fe2', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table5
            WHERE id = '0159d6c7-973f-5e7a-a9a0-d195d0ea6fe2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # insert without specifying id should also work
        sql = """
            INSERT INTO weaviate_test.test_table5 (
                content,metadata,embeddings
            )
            VALUES (
                'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table5
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record + 2

        # insert into a table with a select statement
        sql = """
            INSERT INTO weaviate_test.test_table5 (
                content,metadata,embeddings
            )
            SELECT
                content,metadata,embeddings
            FROM
                weaviate.df
        """
        self.run_sql(sql)
        # check if the data is inserted
        sql = """
            SELECT * FROM weaviate_test.test_table5
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == num_record * 2 + 2

        # insert into a table with a select statement, but wrong columns
        with pytest.raises(Exception):
            sql = """
                INSERT INTO weaviate_test.test_table5
                SELECT
                    content,metadata,embeddings as wrong_column
                FROM
                    weaviate.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, missing metadata column
        sql = """
            INSERT INTO weaviate_test.test_table5
            SELECT
                content,embeddings
            FROM
                weaviate.df
        """
        self.run_sql(sql)

        # insert into a table with a select statement, missing embedding column, shall raise an error
        with pytest.raises(Exception):
            sql = """
                INSERT INTO weaviate_test.test_table5
                SELECT
                    content,metadata
                FROM
                    weaviate.df
            """
            self.run_sql(sql)

        # insert into a table with a select statement, with different embedding dimensions, shall raise an error
        sql = """
            INSERT INTO weaviate_test.test_table5
            SELECT
                content,metadata,embeddings
            FROM
                weaviate.df2
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # insert into a table with existing id, shall raise an error
        sql = """
            INSERT INTO weaviate_test.test_table5 (
                id,content,metadata,embeddings
            )
            VALUES (
                '6ed955c6-506a-5343-9be4-2c0afae02eef', 'this is a test', '{"test": "test"}', '[1.0, 2.0, 3.0]'
            )
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_select_from(self, postgres_handler_mock):
        df = pd.DataFrame(
            {
                "id": [
                    "7fef88f7-411d-5669-b42d-bf5fc7f9b58b",
                    "52524d6e-10dc-5261-aa36-8b2efcbaa5f0",
                ],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test"}, {"test": "test"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "weaviate", tables={"test_table6": df})
        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table6 (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # query a table without any filters
        sql = """
            SELECT * FROM weaviate_test.test_table6
        """
        self.run_sql(sql)

        # query a table with id
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE id = '7fef88f7-411d-5669-b42d-bf5fc7f9b58b'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a search vector, without limit
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a search vector, with limit
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE search_vector = '[1.0, 2.0, 3.0]'
            LIMIT 1
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # query a table with a metadata filter
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # query a table with a metadata filter and a search vector
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test'
            AND search_vector = '[1.0, 2.0, 3.0]'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

    @pytest.mark.xfail(reason="upsert for vectordatabase is not implemented")
    def test_update(self):
        # update a table with a metadata filter
        sql = """
            UPDATE weaviate_test.test_table6
            SET `metadata.test` = 'test2'
            WHERE `metadata.test` = 'test'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update the embeddings
        sql = """
            UPDATE weaviate_test.test_table6
            SET embedding = [3.0, 2.0, 1.0]
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [3.0, 2.0, 1.0]

        # update multiple columns
        sql = """
            UPDATE weaviate_test.test_table6
            SET `metadata.test` = 'test3',
                embedding = [1.0, 2.0, 3.0]
                content = 'this is a test'
            WHERE `metadata.test` = 'test2'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2
        assert ret.embedding[0] == [1.0, 2.0, 3.0]
        assert ret.content[0] == "this is a test"

        # update a table with a search vector filter is not allowed
        sql = """
            UPDATE weaviate_test.test_table6
            SET `metadata.test = 'test2'
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # update a table without any filters is allowed
        sql = """
            UPDATE weaviate_test.test_table6
            SET metadata.test = 'test3'
        """
        self.run_sql(sql)
        # check if the data is updated
        sql = """
            SELECT * FROM weaviate_test.test_table6
            WHERE `metadata.test` = 'test3'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 2

        # update a table with a search vector filter and a metadata filter is not allowed
        sql = """
            UPDATE weaviate_test.test_table6
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
                "id": [
                    "91c274f2-9a0d-5ce6-ac3d-7529f452df21",
                    "0ff1e264-520d-543a-87dd-181a491e667e",
                ],
                "content": ["this is a test", "this is a test"],
                "metadata": [{"test": "test1"}, {"test": "test2"}],
                "embeddings": [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0]],
            }
        )
        self.set_handler(postgres_handler_mock, "weaviate", tables={"test_table7": df})

        # create a table
        sql = """
            CREATE TABLE weaviate_test.test_table7 (
                SELECT * FROM weaviate.df
            )
        """
        self.run_sql(sql)

        # delete from a table with a metadata filter
        sql = """
            DELETE FROM weaviate_test.test_table7
            WHERE `metadata.test` = 'test1'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM weaviate_test.test_table7
            WHERE `metadata.test` = 'test2'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 1

        # delete by id
        sql = """
            DELETE FROM weaviate_test.test_table7
            WHERE id = '0ff1e264-520d-543a-87dd-181a491e667e'
        """
        self.run_sql(sql)
        # check if the data is deleted
        sql = """
            SELECT * FROM weaviate_test.test_table7
            WHERE id = '0ff1e264-520d-543a-87dd-181a491e667e'
        """
        ret = self.run_sql(sql)
        assert ret.shape[0] == 0

        # delete from a table with a search vector filter is not allowed
        sql = """
            DELETE FROM weaviate_test.test_table7
            WHERE search_vector = [1.0, 2.0, 3.0]
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # delete from a table without any filters is not allowed
        sql = """
            DELETE FROM weaviate_test.test_table7
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

import tempfile
import time
from unittest.mock import MagicMock

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from mindsdb.interfaces.storage.db import KnowledgeBase

from .executor_test_base import BaseExecutorTest


class TestKnowledgeBase(BaseExecutorTest):
    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
        return pd.DataFrame(ret.data, columns=columns)

    def wait_predictor(self, project, name):
        # wait
        done = False
        for _ in range(200):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                if ret["STATUS"][0] == "complete":
                    done = True
                    break
                elif ret["STATUS"][0] == "error":
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor wasn't created")

    def setup_method(self):
        super().setup_method()

        vectordatabase_name = "chroma_test"

        # create a vector database table
        tmp_directory = tempfile.mkdtemp()
        self.run_sql(
            f"""
            CREATE DATABASE {vectordatabase_name}
            WITH ENGINE = "chromadb",
            PARAMETERS = {{
                "persist_directory" : "{tmp_directory}"
            }}
        """
        )

        # mock the data
        df = pd.DataFrame(
            {
                "id": ["id1", "id2", "id3"],
                "content": ["content1", "content2", "content3"],
                "metadata": [{"a": 1}, {"b": 2}, {"c": 3}],
                "embeddings": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            }
        )
        postgres_mock = MagicMock()
        self.set_handler(postgres_mock, "pg", tables={"df": df})

        # create the table
        vectordatabase_table_name = "test_table"
        sql = f"""
            CREATE TABLE chroma_test.{vectordatabase_table_name}
            (
                SELECT * FROM pg.df
            )
        """
        self.run_sql(sql)

        # create an embedding model
        embedding_model_name = "test_dummy_embedding"
        self.run_sql(
            f"""
            CREATE MODEL {embedding_model_name}
            PREDICT embeddings
            USING
                engine='langchain_embedding',
                class = 'FakeEmbeddings',
                size = 3,
                input_columns = ['content']
            """
        )

        self.wait_predictor("mindsdb", embedding_model_name)
        self.vector_database_table_name = vectordatabase_table_name
        self.vector_database_name = vectordatabase_name
        self.embedding_model_name = embedding_model_name

    def test_create_kb(self):
        # create a knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """
        self.run_sql(sql)

        # verify the knowledge base is created
        kb_obj = self.db.session.query(KnowledgeBase).filter_by(name="test_kb").first()
        assert kb_obj is not None

        # create a knowledge base from select
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb2
            FROM (
                SELECT content, embeddings, metadata
                FROM {self.vector_database_name}.{self.vector_database_table_name}
            )
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """

        self.run_sql(sql)

        # verify the knowledge base is created
        kb_obj = self.db.session.query(KnowledgeBase).filter_by(name="test_kb2").first()
        assert kb_obj is not None

    def test_drop_kb(self):
        # create a knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """
        self.run_sql(sql)

        # verify the knowledge base is created
        kb_obj = self.db.session.query(KnowledgeBase).filter_by(name="test_kb").first()
        assert kb_obj is not None

        # drop a knowledge base
        sql = """
            DROP KNOWLEDGE BASE test_kb
        """
        self.run_sql(sql)

        # verify the knowledge base is dropped
        kb_obj = self.db.session.query(KnowledgeBase).filter_by(name="test_kb").first()
        assert kb_obj is None

    def test_select_from_kb(self):
        # create the knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """
        self.run_sql(sql)

        # select from the knowledge base without any filters
        sql = """
            SELECT *
            FROM test_kb
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 3

        # select from the knowledge base with an id filter
        sql = """
            SELECT *
            FROM test_kb
            WHERE id = 'id1'
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

        # select from the knowledge base with a metadata filter
        sql = """
            SELECT *
            FROM test_kb
            WHERE
                `metadata.a` = 1
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

        # select with a search query
        sql = """
            SELECT *
            FROM test_kb
            WHERE
                search_query = 'some query'
            LIMIT 1
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

    def insert_into_kb(self):
        # create the knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """
        self.run_sql(sql)

        # insert into the knowledge base using values
        sql = """
            INSERT INTO test_kb
            VALUES (
                'id4',
                'content4',
                '[4, 5, 6]',
                '{"d": 4}'
            )
        """
        self.run_sql(sql)

        # verify the knowledge base is updated
        sql = """
            SELECT *
            FROM test_kb
            WHERE id = 'id4'
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

        # insert into the knowledge base using a select
        sql = """
            INSERT INTO test_kb
            SELECT
                content, metadata
            FROM pg.df
        """
        self.run_sql(sql)

        # verify the knowledge base is updated
        sql = """
            SELECT *
            FROM test_kb
        """

        df = self.run_sql(sql)
        assert df.shape[0] == 6

    @pytest.mark.skip(reason="Not implemented")
    def test_update_kb(self):
        ...

    def test_delete_from_kb(self):
        # create the knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """

        self.run_sql(sql)

        # delete with id filter
        sql = """
            DELETE FROM test_kb
            WHERE id = 'id1'
        """
        self.run_sql(sql)

        # verify the knowledge base is updated
        sql = """
            SELECT *
            FROM test_kb
            WHERE id = 'id1'
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 0

        # delete with metadata filter
        sql = """
            DELETE FROM test_kb
            WHERE `metadata.b` = 2
        """
        self.run_sql(sql)

        # verify the knowledge base is updated
        sql = """
            SELECT *
            FROM test_kb
            WHERE id = 'id2'
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 0

        # delete from the knowledge base without any filters is not allowed
        sql = """
            DELETE FROM test_kb
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        # delete with search query is not allowed
        sql = """
            DELETE FROM test_kb
            WHERE search_query = 'some query'
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_show_knowledge_bases(self):
        # create the knowledge base
        sql = f"""
            CREATE KNOWLEDGE BASE test_kb
            MODEL {self.embedding_model_name}
            STORAGE {self.vector_database_name}.{self.vector_database_table_name}
        """
        self.run_sql(sql)

        # show knowledge bases
        sql = """
            SHOW KNOWLEDGE BASES
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

import tempfile
import time
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from mindsdb.interfaces.storage.db import RAG

from .executor_test_base import BaseExecutorDummyLLM


class TestRAG(BaseExecutorDummyLLM):
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

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def setup_method(self, method, mock_handler):
        super().setup_method()

        vector_database_name = "chroma_test"

        # create a vector database table
        tmp_directory = tempfile.mkdtemp()
        self.run_sql(
            f"""
                 CREATE DATABASE {vector_database_name}
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
                "metadata": [
                    '{"datasource": "web", "some_field": "some_value"}',
                    '{"datasource": "web"}',
                    '{"datasource": "web"}',
                ],
                "embeddings": [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            }
        )

        self.set_handler(mock_handler, "pg", tables={"df": df})

        # create the table
        vector_database_table_name = "test_table"
        sql = f"""
                 CREATE TABLE chroma_test.{vector_database_table_name}
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

        # create a knowledge base
        kb_name = "kb_test"

        sql = f"""
            CREATE KNOWLEDGE BASE {kb_name}
            USING
            STORAGE = {vector_database_name}.{vector_database_table_name},
            MODEL = {embedding_model_name}
        """

        self.run_sql(sql)

        # create a llm

        llm_name = "llm_test"

        sql = f"""
                CREATE model {llm_name}
                from pg (select * from df)
                PREDICT answer
                using engine='dummy_llm'
                """

        self.run_sql(sql)

        # wait for the predictor to be created
        self.wait_predictor("mindsdb", llm_name)

        self.vector_database_table_name = vector_database_table_name
        self.vector_database_name = vector_database_name
        self.embedding_model_name = embedding_model_name
        self.kb_name = kb_name
        self.llm_name = llm_name
        self.database_path = tmp_directory

    def teardown_method(self, method):
        # drop the vector database, the llm and the knowledge base

        self.run_sql(f"DROP Table {self.vector_database_table_name}")
        self.run_sql(f"DROP DATABASE {self.vector_database_name}")
        self.run_sql(f"DROP MODEL {self.embedding_model_name}")
        self.run_sql(f"DROP MODEL {self.llm_name}")
        self.run_sql(f"DROP KNOWLEDGE BASE {self.kb_name}")

    def test_create_rag(self):
        # create a RAG
        sql = f"""
            CREATE RAG test_rag
            USING
            llm = {self.llm_name},
            knowledge_base = {self.kb_name}
        """
        self.run_sql(sql)

        # verify the RAG is created
        rag_obj = self.db.session.query(RAG).filter_by(name="test_rag").first()
        assert rag_obj is not None

        # create a RAG with invalid llm and knowledge base should throw an exception
        sql = f"""
            CREATE RAG test_rag2
            USING
            llm = invalid_model_name,
            knowledge_base = {self.kb_name}
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        sql = f"""
            CREATE RAG test_rag3
            USING
            LLM = {self.llm_name},
            knowledge_base = invalid_storage_name
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_drop_rag(self):
        # create a RAG
        sql = f"""
            CREATE RAG test_rag
            USING
            llm = {self.llm_name},
            knowledge_base = {self.kb_name}
        """
        self.run_sql(sql)

        # verify the RAG is created
        rag_obj = self.db.session.query(RAG).filter_by(name="test_rag").first()
        assert rag_obj is not None

        # drop a RAG base
        sql = """
            DROP RAG test_rag
        """
        self.run_sql(sql)

        # verify the RAG is dropped
        rag_obj = self.db.session.query(RAG).filter_by(name="test_rag").first()
        assert rag_obj is None

    def test_select_from_rag(self):
        # create a RAG
        sql = f"""
            CREATE RAG test_rag
            USING
            llm = {self.llm_name},
            knowledge_base = {self.kb_name}
        """
        self.run_sql(sql)

        # select all without where clause should fail
        sql = """
            SELECT *
            FROM test_rag
        """

        with pytest.raises(Exception):
            self.run_sql(sql)

        # select with a valid where clause
        sql = """
            SELECT *
            FROM test_rag
            WHERE
                question = 'what is the answer?'
        """
        df = self.run_sql(sql)
        assert df.shape[0] == 1

    @pytest.mark.skip(reason="Not implemented")
    def test_update_rag(self):
        ...

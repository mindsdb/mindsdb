import tempfile
import time

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from mindsdb.interfaces.storage.db import RAG

from .executor_test_base import BaseExecutorDummyLLM


class TestRAG(BaseExecutorDummyLLM):
    def wait_predictor(self, project, name, filter=None):
        # wait
        done = False
        for attempt in range(200):
            sql = f"select * from {project}.models_versions where name='{name}'"
            if filter is not None:
                for k, v in filter.items():
                    sql += f" and {k}='{v}'"
            ret = self.run_sql(sql)
            if not ret.empty:
                if ret['STATUS'][0] == 'complete':
                    done = True
                    break
                elif ret['STATUS'][0] == 'error':
                    break
            time.sleep(0.5)
        if not done:
            raise RuntimeError("predictor didn't created")

    def run_sql(self, sql, throw_error=True, database='mindsdb'):
        self.command_executor.session.database = database
        ret = self.command_executor.execute_command(
            parse_sql(sql, dialect='mindsdb')
        )
        if throw_error:
            assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name
                for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    def setup_method(self):

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

        self.save_file("df", df)

        # create the table
        vector_database_table_name = "test_table"
        sql = f"""
                 CREATE TABLE chroma_test.{vector_database_table_name}
                 (
                     SELECT * FROM files.df
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

        self.wait_predictor("mindsdb", kb_name)

        # create a llm

        llm_name = "llm_test"

        sql = f"""
                CREATE model {llm_name}
                PREDICT answer
                using engine='dummy_llm',
                join_learn_process=true,
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

    def test_create_rag(self):
        # create a RAG
        sql = f"""
            CREATE RAG test_rag
            USING
            llm = {self.llm_name},
            knowledge_base_store = {self.kb_name}
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
            knowledge_base_store = {self.kb_name}
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

        sql = f"""
            CREATE RAG test_rag3
            USING
            LLM = {self.llm_name},
            knowledge_base_store = invalid_storage_name
        """
        with pytest.raises(Exception):
            self.run_sql(sql)

    def test_drop_rag(self):
        # create a RAG
        sql = f"""
            CREATE RAG test_rag
            USING
            llm = {self.llm_name},
            knowledge_base_store = {self.kb_name}
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
            knowledge_base_store = {self.kb_name}
        """
        self.run_sql(sql)

        # select all without where clause should fail

        # todo it should not be possible to run select without where

        # sql = """
        #     SELECT *
        #     FROM test_rag
        # """
        #
        # with pytest.raises(Exception):
        #     self.run_sql(sql)

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

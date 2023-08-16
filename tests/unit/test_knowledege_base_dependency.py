import os
import time
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from mindsdb.interfaces.knowledge_base import service
from mindsdb.interfaces.storage.db import KnowledgeBase

from .executor_test_base import BaseExecutorTest


class TestDependencyManagement(BaseExecutorTest):
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

    def run_sql(self, sql):
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None
        if ret.data is not None:
            columns = [
                col.alias if col.alias is not None else col.name for col in ret.columns
            ]
            return pd.DataFrame(ret.data, columns=columns)

    @pytest.mark.skipIf(
        "OPENAI_API_KEY" not in os.environ,
        "OPENAI_API_KEY is not set, skipping test_dependency_injection",
    )
    def test_dependency_injection(self):
        """
        Test dependency injection
        """
        # create a chromadb database integration
        self.run_sql("create project kb_test")
        self.run_sql(
            """
            CREATE DATABASE my_chroma
            WITH engine = 'chromadb';
            """
        )

        # check the chromadb database integration is created correctly
        assert self.db.Integration.query.filter_by(name="my_chroma").count() == 1

        # create a openai embedding model
        self.run_sql(
            """
            CREATE MODEL kb_test.my_embedding
            PREDICT embedding_vector
            USING
                engine = 'openai',
                mode = 'embedding',
                question_column = 'content';
            """
        )

        # wait for the model to be created
        self.wait_predictor("kb_test", "my_embedding")

        # check the predictor is created correctly
        assert self.db.Predictor.query.filter_by(name="my_embedding").count() == 1

        # create a knowledege base record by referring to the chromadb database integration and openai embedding model
        project_id = self.db.Project.query.filter_by(name="kb_test").first().id
        kb = KnowledgeBase(
            name="test knowledge base",
            company_id=-1,
            project_id=project_id,
            embedding_model_id=self.db.Predictor.query.filter_by(name="my_embedding")
            .first()
            .id,
            vector_database_id=self.db.Integration.query.filter_by(name="my_chroma")
            .first()
            .id,
            vector_database_table_name="test",
        )

        # use self.db to replace the db module in mindsdb.interfaces.knowledge_base.service
        # the db object is imported as
        # import mindsdb.interfaces.knowledge_base.service.db
        # under the mindsdb.interfaces.knowledge_base.service module
        with patch.object(service, "db", self.db):
            # let the service.IntegrationController to use the self.sql_session.integration_controller
            with patch.object(
                service,
                "IntegrationController",
                lambda *args, **kwargs: self.sql_session.integration_controller,
            ):
                kb_service = service.KnowledgeBaseService(kb)

                # test the embedding handler and vector database handler are created correctly
                embedding_model_handler = kb_service.embedding_model_handler
                vector_database_handler = kb_service.vector_database_handler

                assert embedding_model_handler is not None
                assert vector_database_handler is not None

                # test the embedding model is working correctly
                embeddings = embedding_model_handler.predict(
                    pd.DataFrame(
                        {
                            "content": [
                                "I am a sentence",
                                "I am another sentence",
                            ]
                        }
                    )
                )

                assert embeddings.shape[0] == 2
                assert "embedding_vector" in embeddings.columns

                # test the vector database is working correctly
                status = vector_database_handler.check_connection()
                assert status.success

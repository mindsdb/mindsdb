import os
import time
from unittest.mock import patch

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from ..executor_test_base import BaseExecutorTest


class TestLangchainEmbedding(BaseExecutorTest):
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

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_dummy_embedding(self, mock_handler):
        self.run_sql("create database proj")
        # create  the model
        self.run_sql(
            """
            CREATE MODEL proj.test_dummy_embedding
            PREDICT content
            USING
                engine='langchain_embedding',
                class = 'FakeEmbeddings',
                size = 512
            """
        )

        self.wait_predictor("proj", "test_dummy_embedding")

        # predictions
        # one line
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding
            WHERE content='hello'
            """
        )
        assert "content" in ret.columns
        assert "embeddings" in ret.columns
        # the embeddings should be a list of 512 floats
        assert len(ret["embeddings"][0]) == 512

        # multiple lines
        # insert data
        df = pd.DataFrame(
            [
                ["hello"],
                ["world"],
                ["foo"],
                ["bar"],
            ],
            columns=["content"],
        )
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # query
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding
            JOIN pg.df
            """
        )

        assert "content" in ret.columns
        assert "embeddings" in ret.columns
        assert ret.shape[0] == 4

    def test_user_friends_embedding_model_name(self):
        self.run_sql("create database proj")
        # create  the model
        self.run_sql(
            """
            CREATE MODEL proj.test_dummy_embedding
            PREDICT content
            USING
                engine='langchain_embedding',
                class = 'fake', -- a more user friendly name
                size = 512
            """
        )

        self.wait_predictor("proj", "test_dummy_embedding")

    # skip if there is no openai key defined in the env
    @pytest.mark.skipif(
        "OPENAI_API_KEY" not in os.environ,
        reason="OPENAI_API_KEY env variable is not defined",
    )
    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_openai_embedding(self, mock_handler):
        self.run_sql("create database proj")
        # create the model
        self.run_sql(
            """
            CREATE MODEL proj.test_openai_embedding
            PREDICT content
            USING
                engine='langchain_embedding',
                class = 'openai'
            """
        )

        self.wait_predictor("proj", "test_openai_embedding")

        # single line prediction
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_openai_embedding
            WHERE content='hello'
            """
        )

        assert "content" in ret.columns
        assert "embeddings" in ret.columns

        # multiple lines
        # insert data
        df = pd.DataFrame(
            [
                ["hello"],
                ["world"],
                ["foo"],
                ["bar"],
            ],
            columns=["content"],
        )
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # query
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_openai_embedding
            JOIN pg.df
            """
        )

        assert "content" in ret.columns
        assert "embeddings" in ret.columns
        assert ret.shape[0] == 4

    def test_huggingface_embedding(self):
        ...

    def test_missing_class_name(self):
        self.run_sql("create database proj")
        with pytest.raises(Exception):
            self.run_sql(
                """
                CREATE MODEL proj.test_missing_class_name
                USING
                    engine='langchain_embedding',
                    size = 512
                """
            )

    def test_wrong_class_name(self):
        self.run_sql("create database proj")
        with pytest.raises(Exception):
            self.run_sql(
                """
                CREATE MODEL proj.test_wrong_class_name
                USING
                    engine='langchain_embedding',
                    class = 'SomethingDoesntExist',
                    size = 512
                """
            )

    def test_wrong_arguments(self):
        self.run_sql("create database proj")
        with pytest.raises(Exception):
            self.run_sql(
                """
                CREATE MODEL proj.test_wrong_arguments
                USING
                    engine='langchain_embedding',
                    class = 'FakeEmbeddings',
                    wrong_argument_name = 512
                """
            )

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
            PREDICT embeddings_output_column
            USING
                engine='langchain_embedding',
                class = 'FakeEmbeddings',
                size = 512,
                input_columns = ['content']
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
        assert "embeddings_output_column" in ret.columns
        # the embeddings should be a list of 512 floats
        assert len(ret["embeddings_output_column"][0]) == 512

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
        assert "embeddings_output_column" in ret.columns
        assert ret.shape[0] == 4

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_embed_multiple_columns(self, mock_handler):
        self.run_sql("create database proj")
        # create  the model
        # with multiple input columns
        self.run_sql(
            """
            CREATE MODEL proj.test_dummy_embedding
            PREDICT embeddings
            USING
                engine='langchain_embedding',
                class = 'fake', -- a more user friendly name
                size = 512,
                input_columns = ['content1', 'content2']
            """
        )

        self.wait_predictor("proj", "test_dummy_embedding")

        # predictions
        # one line
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding
            WHERE content1='hello'
            AND content2='world'
            """
        )

        assert "content1" in ret.columns
        assert "content2" in ret.columns
        assert "embeddings" in ret.columns

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "content1": ["hello", "world", "foo", "bar"],
                "content2": ["world", "hello", "bar", "foo"],
            }
        )
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # query
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding
            JOIN pg.df
            """
        )

        assert "content1" in ret.columns
        assert "content2" in ret.columns
        assert "embeddings" in ret.columns
        assert ret.shape[0] == 4

        # if the input missing columns, it should throw an error
        with pytest.raises(Exception):
            self.run_sql(
                """
                SELECT * FROM proj.test_dummy_embedding
                WHERE content1='hello'
                """
            )

        # if the input missing columns, it should throw an error
        with pytest.raises(Exception):
            df2 = pd.DataFrame(
                {
                    "content1": ["hello", "world", "foo", "bar"],
                }
            )
            self.set_handler(mock_handler, name="pg", tables={"df": df2})
            self.run_sql(
                """
                SELECT * FROM proj.test_dummy_embedding
                JOIN pg.df2
                """
            )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_no_input_columns(self, mock_handler):
        self.run_sql("create database proj")

        df = pd.DataFrame(
            {
                "id": [1, 2, 3, 4],
                "content1": ["hello", "world", "foo", "bar"],
                "content2": ["world", "hello", "bar", "foo"],
            }
        )
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # create the model with no input columns specified should use
        # all columns when embedding the documents
        self.run_sql(
            """
            CREATE MODEL proj.test_dummy_embedding2
            PREDICT embeddings
            USING
                engine='langchain_embedding',
                class = 'fake', -- a more user friendly name
                size = 512
            """
        )

        self.wait_predictor("proj", "test_dummy_embedding2")

        # predictions
        # one line
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding2
            WHERE content1='hello'
            AND content2='world'
            AND id = 1
            """
        )

        assert "content1" in ret.columns
        assert "content2" in ret.columns
        assert "id" in ret.columns or "`id`" in ret.columns
        assert "embeddings" in ret.columns

        # multiple lines
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding2
            JOIN pg.df
            """
        )

        assert "content1" in ret.columns
        assert "content2" in ret.columns
        assert ret.shape[0] == 4

        # create the model with no input columns specified,
        # but with a given from dataframe should use all the columns
        # from the dataframe when embedding the documents
        ret = self.run_sql(
            """
            CREATE MODEL proj.test_dummy_embedding3
            FROM pg (
                SELECT *, NULL as embeddings FROM df  -- this requires an empty column called embeddings
            )
            PREDICT embeddings
            USING
                engine='langchain_embedding',
                class = 'fake', -- a more user friendly name
                size = 512
            """
        )

        self.wait_predictor("proj", "test_dummy_embedding3")

        # input columns == ['id', 'content1', 'content2']
        # predictions
        # one line
        ret = self.run_sql(
            """
            SELECT * FROM proj.test_dummy_embedding3
            WHERE content1='hello'
            AND content2='world'
            AND id = 1  -- looks like 'id' will be quoted
            """
        )

        # missing columns id
        with pytest.raises(Exception):
            self.run_sql(
                """
                SELECT * FROM proj.test_dummy_embedding3
                WHERE content1='hello'
                AND content2='world'
                """
            )

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
                    class = 'SomethingDoesNotExist',
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

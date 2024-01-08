import os
import time

import pandas as pd
import pytest
from mindsdb_sql import parse_sql

from tests.unit.executor_test_base import BaseExecutorTest

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY

USER_MESSAGE = "Write a short poem about the sky"
MESSAGE = [{"content": USER_MESSAGE, "role": "user"}]
MESSAGES = [[{"content": USER_MESSAGE, "role": "user"}], [{"content": "The sky is blue", "role": "user"}]]


class TestLiteLLM(BaseExecutorTest):
    def wait_predictor(self, project, name):
        # wait
        done = False
        for attempt in range(200):
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

    def test_completion_without_prompt_template(self):
        # create project
        self.run_sql("create database proj")

        # simple completion with openai using prompt on predict

        self.run_sql(
            f"""
            CREATE MODEL proj.test_litellm_handler_on_completion_openai
            PREDICT text
            USING
            engine="litellm",
            model="gpt-3.5-turbo",
            api_key='{OPENAI_API_KEY}'
            """
        )

        self.wait_predictor("proj", "test_litellm_handler_on_completion_openai")

        # run predict
        ret = self.run_sql(
            """
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai
            where text = "I like to eat" and mock_response = "test"
            """
        )

        assert ret["result"][0]

        # completion with openai using messages in predict

        # run predict
        ret = self.run_sql(
            f"""
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai
            where messages = "{MESSAGE}" and mock_response = "test"
            """
        )

        assert ret["result"][0]

        # completion with openai using multiple messages in predict using batch_completion

        # run predict
        ret = self.run_sql(
            f"""
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai
            where messages = "{MESSAGES}"
            """
        )

        # check there are two results
        assert ret["result"].shape[0] == 2

        # completion with messages in predict with other args should fail

        # run predict
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                SELECT *
                FROM proj.test_litellm_handler_on_completion_openai
                where messages = "{MESSAGES}" and temperature = 0.5
                """
            )

    def test_completion_openai_with_prompt_template(self):

        # create project
        self.run_sql("create database proj")

        # completion with openai using prompt_template in create with single format variable

        self.run_sql(
            f"""
            CREATE MODEL proj.test_litellm_handler_on_completion_openai_prompt_template
            PREDICT text
            USING
            engine="litellm",
            model="gpt-3.5-turbo",
            api_key='{OPENAI_API_KEY}',
            prompt_template="I like to eat {{text}}"
            """
        )

        self.wait_predictor("proj", "test_litellm_handler_on_completion_openai_prompt_template")

        # run predict

        ret = self.run_sql(
            """
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai_prompt_template
            where text = "pizza" and mock_response = "test"
            """
        )

        assert ret["result"][0]

        # completion with openai using prompt_template in create with multiple format variables

        self.run_sql(
            f"""
            CREATE MODEL proj.test_litellm_handler_on_completion_openai_prompt_template_multiple
            PREDICT text
            USING
            engine="litellm",
            model="gpt-3.5-turbo",
            api_key='{OPENAI_API_KEY}',
            prompt_template="I like to eat {{text1}} and {{text2}}"
            """
        )

        self.wait_predictor("proj", "test_litellm_handler_on_completion_openai_prompt_template_multiple")

        # run predict

        ret = self.run_sql(
            """
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai_prompt_template_multiple
            where text1 = "pizza" and text2 = "pasta" and mock_response = "test"
            """
        )

        assert ret["result"][0]

        # use prompt_template in predict

        self.run_sql(
            """
            CREATE MODEL proj.test_litellm_handler_on_completion_openai_prompt_template_predict
            PREDICT text
            USING
            engine="litellm",
            model="gpt-3.5-turbo",
            api_key='{OPENAI_API_KEY}'
            """
        )

        self.wait_predictor("proj", "test_litellm_handler_on_completion_openai_prompt_template_predict")

        # run predict

        ret = self.run_sql(
            """
            SELECT *
            FROM proj.test_litellm_handler_on_completion_openai_prompt_template_predict
            where text = "pizza" and prompt_template = "I like to eat {{text}}" and mock_response = "test"
            """
        )

        assert ret["result"][0]

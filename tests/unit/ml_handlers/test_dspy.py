import os

import pytest

from ..executor_test_base import BaseExecutorTest

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")



class TestDSPy(BaseExecutorTest):
    """Test Class for Langchain Integration Testing"""
    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_mdb_read(self):
        self.run_sql(
            f"""
           create model proj.test_mdb_model
           predict answer
           using
             engine='dspy',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             openai_api_key='{OPENAI_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_mdb_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_mdb_model
            WHERE question='Can you get a list of all available MindsDB models?'
        """
        )
        assert "test_mdb_model" in result_df['answer'].iloc[0].lower()

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_default_provider(self):
        self.run_sql(
            f"""
           create model proj.test_conversational_model
           predict answer
           using
             engine='dspy',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             openai_api_key='{OPENAI_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_conversational_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_conversational_model
            WHERE question='What is the capital of Sweden?'
        """
        )
        assert "stockholm" in result_df['answer'].iloc[0].lower()

    def test_describe(self):
        pass
        # self.run_sql(
        #     """
        #    create model proj.test_describe_model
        #    predict answer
        #    using
        #      engine='dspy',
        #      prompt_template='Answer the user in a useful way: {{question}}';
        # """
        # )
        # self.wait_predictor("proj", "test_describe_model")
        # result_df = self.run_sql('DESCRIBE proj.test_describe_model')
        # assert not result_df.empty
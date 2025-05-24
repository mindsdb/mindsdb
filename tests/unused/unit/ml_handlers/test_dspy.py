import os


import ollama
import pytest
from ..executor_test_base import BaseExecutorTest


OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


def ollama_model_exists(model_name: str) -> bool:

    try:
        ollama.show(model_name)
        return True
    except Exception:
        return False


class TestDSPy(BaseExecutorTest):

    """Test Class for DSPy Integration Testing"""
    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_default_provider(self):

        self.run_sql(
            f"""
            CREATE ML_ENGINE dspy_engine
            FROM dspy
            USING
                openai_api_key = '{OPENAI_API_KEY}';
            """
        )

        self.run_sql(
            """
           create model proj.test_conversational_model
           predict answer
           using
            engine='dspy_engine',
            provider = 'openai',
            model_name = 'gpt-4',
            mode = 'conversational',
            user_column = 'question',
            assistant_column = 'answer',
            prompt_template='Answer the user in a useful way';
        """
        )
        self.wait_predictor("proj", "test_conversational_model")
        result_df = self.run_sql(
            """
            SELECT question, answer
            FROM proj.test_conversational_model
            WHERE question='What is the capital of Sweden?;'
        """
        )
        assert "stockholm" in result_df['answer'].iloc[0].lower()

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_default_provider2(self):

        self.run_sql(
            f"""
            CREATE ML_ENGINE dspy_engine
            FROM dspy
            USING
                openai_api_key = '{OPENAI_API_KEY}';
            """
        )

        self.run_sql(
            """
           create model proj.test_conversational_model
           predict answer
           using
            engine='dspy_engine',
            provider = 'openai',
            model_name = 'gpt-3.5-turbo',
            mode = 'conversational',
            user_column = 'question',
            assistant_column = 'answer',
            prompt_template='Answer the user in a useful way';
        """
        )
        self.wait_predictor("proj", "test_conversational_model")
        result_df = self.run_sql(
            """
            SELECT question, answer
            FROM proj.test_conversational_model
            WHERE question='What is 3 + 4?;'
        """
        )
        assert "7" in result_df['answer'].iloc[0].lower()

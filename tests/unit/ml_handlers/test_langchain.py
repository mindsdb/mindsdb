import os

import ollama
import pandas as pd
import pytest

from ..ml_handlers.base_ml_test import BaseMLAPITest

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


def ollama_model_exists(model_name: str) -> bool:
    try:
        ollama.show(model_name)
        return True
    except Exception:
        return False


class TestLangchain(BaseMLAPITest):
    """Test Class for Langchain Integration Testing"""

    def setup_method(self, method):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_conversational(self):
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
        ]})
        self.set_data('df', df)

        self.run_sql(
            f"""
           create model proj.test_conversational_model
           predict answer
           using
             engine='langchain',
             mode='conversational',
             model_name='gpt-4-0125-preview',
             user_column='question',
             assistant_column='answer',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             openai_api_key='{self.get_api_key('OPENAI_API_KEY')}';
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

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_mdb_read(self):
        df = pd.DataFrame.from_dict({"question": [
            "Can you get a list of all available MindsDB models?",
        ]})
        self.set_data('df', df)

        self.run_sql(
            f"""
           create model proj.test_mdb_model
           predict answer
           using
             engine='langchain',
             mode='conversational',
             model_name='gpt-4-0125-preview',
             user_column='question',
             assistant_column='answer',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             openai_api_key='{self.get_api_key('OPENAI_API_KEY')}';
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

    @pytest.mark.skipif(not ollama_model_exists('mistral'), reason='Make sure the mistral model is available locally by running `ollama pull mistral`')
    def test_ollama_provider(self):
        self.run_sql(
            """
           create model proj.test_ollama_model
           predict answer
           using
             engine='langchain',
             mode='conversational',
             model_name='mistral',
             user_column='question',
             assistant_column='answer',
             prompt_template='Answer the user in a useful way: {{{{question}}}}'
            """
        )
        self.wait_predictor("proj", "test_ollama_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_ollama_model
            WHERE question='What is the capital of British Columbia, Canada?'
        """
        )
        assert "victoria" in result_df['answer'].iloc[0].lower()

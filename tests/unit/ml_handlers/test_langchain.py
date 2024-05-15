import os

import ollama
import pytest

from ..executor_test_base import BaseExecutorTest

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
ANYSCALE_API_KEY = os.environ.get("ANYSCALE_API_KEY")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")


def ollama_model_exists(model_name: str) -> bool:
    try:
        ollama.show(model_name)
        return True
    except Exception:
        return False


class TestLangchain(BaseExecutorTest):
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
             engine='langchain',
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
             engine='langchain',
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

    @pytest.mark.skipif(ANTHROPIC_API_KEY is None, reason='Missing Anthropic API key (ANTHROPIC_API_KEY env variable)')
    def test_anthropic_provider(self):
        self.run_sql(
            f"""
           create model proj.test_anthropic_langchain_model
           predict answer
           using
             engine='langchain',
             model_name='claude-2.1',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             anthropic_api_key='{ANTHROPIC_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_anthropic_langchain_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_anthropic_langchain_model
            WHERE question='What is the capital of Sweden?'
        """
        )
        assert "stockholm" in result_df['answer'].iloc[0].lower()

    @pytest.mark.skipif(not ollama_model_exists('mistral'), reason='Make sure the mistral model is available locally by running `ollama pull mistral`')
    def test_ollama_provider(self):
        self.run_sql(
            """
           create model proj.test_ollama_model
           predict answer
           using
             engine='langchain',
             model_name='mistral',
             prompt_template='Answer the user in a useful way: {{question}}'
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

    @pytest.mark.skipif(ANYSCALE_API_KEY is None, reason='Missing Anyscale API key (ANYSCALE_API_KEY env variable)')
    def test_anyscale_provider(self):
        self.run_sql(
            f"""
           create model proj.test_anyscale_langchain_model
           predict answer
           using
             engine='langchain',
             provider='anyscale',
             model_name='meta-llama/Llama-2-7b-chat-hf',
             prompt_template='Answer the user in a useful way: {{{{question}}}}',
             anyscale_api_key='{ANYSCALE_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_anyscale_langchain_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_anyscale_langchain_model
            WHERE question='What is the capital of Sweden?'
        """
        )
        assert "stockholm" in result_df['answer'].iloc[0].lower()

    def test_describe(self):
        self.run_sql(
            """
           create model proj.test_describe_model
           predict answer
           using
             engine='langchain',
             prompt_template='Answer the user in a useful way: {{question}}';
        """
        )
        self.wait_predictor("proj", "test_describe_model")
        result_df = self.run_sql('DESCRIBE proj.test_describe_model')
        assert not result_df.empty

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_prompt_template_args(self):
        self.run_sql(
            f"""
           create model proj.test_prompt_template_model
           predict answer
           using
             engine='langchain',
             prompt_template='Your name is {{{{name}}}}. Answer the user in a useful way: {{{{question}}}}',
             openai_api_key='{OPENAI_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_prompt_template_model")

        agent_name = 'professor farnsworth'
        result_df = self.run_sql(
            f"""
            SELECT answer
            FROM proj.test_prompt_template_model
            WHERE question='What is your name?' AND name='{agent_name}'
        """
        )
        assert agent_name in result_df['answer'].iloc[0].lower()

    @pytest.mark.skipif(OPENAI_API_KEY is None, reason='Missing OpenAI API key (OPENAI_API_KEY env variable)')
    def test_simple_vectordb_retrieval_skill(self):

        self.run_sql(
            """
           create model proj.test_retrieval_model
           predict answer
           using
             engine='langchain',
             mode='retrieval'
        """
        )

        self.wait_predictor("proj", "test_retrieval_model")

        # create a retrieval skill
        self.run_sql(
            """
            CREATE SKILL proj.retrieval_skill
            using
            type='retrieval',
            name='country_info_search',
            description='countries';
        """
        )

        # create a retrieval agent
        self.run_sql(
            """
           create agent proj.retrieval_agent
           using
           model='test_retrieval_model',
           skills=['retrieval_skill'];
        """
        )
        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.retrieval_agent
            WHERE question='What is the capital of Sweden?'
        """
        )
        assert "stockholm" in result_df['answer'].iloc[0].lower()

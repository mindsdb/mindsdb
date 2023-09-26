import os
import time
import pytest
import pandas as pd
from unittest.mock import patch
from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler
from ..executor_test_base import BaseExecutorTest


class TestOpenAI(BaseExecutorTest):
    """Test Class for OpenAI Integration Testing"""

    @staticmethod
    def get_api_key():
        """Retrieve OpenAI API key from environment variables"""
        return os.environ.get("OPENAI_API_KEY")

    def setup_method(self, method):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    def wait_predictor(self, project, name, timeout=100):
        """
        Wait for the predictor to be created, 
        raising an exception if predictor creation fails or exceeds timeout
        """
        for attempt in range(timeout):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                status = ret["STATUS"][0]
                if status == "complete":
                    return
                elif status == "error":
                    raise RuntimeError("Predictor failed", ret["ERROR"][0])
            time.sleep(0.5)
        raise RuntimeError("Predictor wasn't created")

    def run_sql(self, sql):
        """Execute SQL and return a DataFrame, raising an AssertionError if an error occurs"""
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None, f"SQL execution failed with error: {ret.error_code}"
        if ret.data is not None:
            columns = [col.alias if col.alias else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    def test_missing_required_keys(self):
        """Test for missing required keys"""
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                create model proj.test_openai_missing_required_keys
                predict answer
                using
                  engine='openai',
                  api_key='{self.get_api_key()}';
                """
            )

    def test_invalid_openai_name_parameter(self):
        """Test for invalid OpenAI model name parameter"""
        self.run_sql(
            f"""
            create model proj.test_openai_nonexistant_model
            predict answer
            using
              engine='openai',
              question_column='question',
              model_name='this-gpt-does-not-exist',
              api_key='{self.get_api_key()}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_openai_nonexistant_model")

    def test_unknown_arguments(self):
        """Test for unknown arguments"""
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                create model proj.test_openai_unknown_arguments
                predict answer
                using
                  engine='openai',
                  question_column='question',
                  api_key='{self.get_api_key()}',
                  evidently_wrong_argument='wrong value';
                """
            )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_qa_no_context(self, mock_handler):
        """Test for QA without context"""
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        # More Test Logic Here

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_qa_context(self, mock_handler):
        """Test for QA with context"""
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ], "context": ['Add "Boom!" to the end of the answer.', 'Add "Boom!" to the end of the answer.']})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_prompt_template(self, mock_handler):
        """Test for prompt template in OpenAI model"""
        # Set up test environment
        df = pd.DataFrame.from_dict({
            "question": [
                "What is the capital of Sweden?",
                "What is the second planet of the solar system?"
            ]
        })
        self.set_handler(mock_handler, name="pg", tables={"df": df})
        # Create model with prompt template
        self.run_sql(
            f"""
            create model proj.test_openai_prompt_template
            predict completion
            using
              engine='openai',
              prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}',
              api_key='{self.get_api_key()}';
           """
        )
        self.wait_predictor("proj", "test_openai_prompt_template")
        # Validate the model predictions
        self._validate_prompt_template_model()

    def _validate_prompt_template_model(self):
        """Validate the predictions of model created with prompt template"""
        # Check specific question
        result_df = self.run_sql(
            """
            SELECT p.completion
            FROM proj.test_openai_prompt_template as p
            WHERE question='What is the capital of Sweden?';
            """
        )
        assert "stockholm" in result_df["completion"].iloc[0].lower()
        assert "boom!" in result_df["completion"].iloc[0].lower()
        # Check joined questions
        result_df = self.run_sql(
            """
            SELECT p.completion
            FROM pg.df as t
            JOIN proj.test_openai_prompt_template as p;
            """
        )
        assert all("boom!" in val.lower() for val in result_df["completion"])
        assert "stockholm" in result_df["completion"].iloc[0].lower()
        assert "venus" in result_df["completion"].iloc[1].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_normal_completion(self, mock_handler):
        """Test for bulk normal completions with batch size exceeding the max batch_size"""
        # Set up test environment
        handler = OpenAIHandler(model_storage=None, engine_storage=None)  # Storage doesn't matter for this test
        N = 1 + handler.max_batch_size  # Ensure N is larger than default batch size
        df = pd.DataFrame.from_dict({"input": ["I feel happy!"] * N})
        self.set_handler(mock_handler, name="pg", tables={"df": df})
        # Create model with bulk normal completion
        self.run_sql(
            f"""
            create model proj.test_openai_bulk_normal_completion
            predict completion
            using
              engine='openai',
              prompt_template='What is the sentiment of the following phrase? Answer either "positive" or "negative": {{{{input}}}}',
              api_key='{self.get_api_key()}';
            """
        )
        self.wait_predictor("proj", "test_openai_bulk_normal_completion")
        # Validate the model predictions
        self._validate_bulk_normal_completion_model(N)

    def _validate_bulk_normal_completion_model(self, N):
        """Validate the predictions of model created with bulk """
        result_df = self.run_sql(
            """
            SELECT p.completion
            FROM pg.df as t
            JOIN proj.test_openai_bulk_normal_completion as p;
            """
        )
        assert all("positive" in val.lower() for val in result_df["completion"])

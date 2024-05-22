import os
import pytest
import pandas as pd
from unittest.mock import patch

from tests.unit.ml_handlers.base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('MDB_TEST_MDB_OPENAI_API_KEY') is None, reason='Missing API key!')
class TestOpenAI(BaseMLAPITest):
    """
    Integration tests for the OpenAI handler.
    """

    def setup_method(self):
        """
        Setup test environment by creating a project and an OpenAI engine.
        """
        super().setup_method()
        self.run_sql("CREATE DATABASE proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE openai_engine
            FROM openai
            USING
            openai_api_key = '{self.get_api_key('MDB_TEST_MDB_OPENAI_API_KEY')}';
            """
        )

    def test_create_model_raises_exception_with_unsupported_model(self):
        """
        Test if CREATE MODEL raises an exception with an unsupported model.
        """
        self.run_sql(
            f"""
            CREATE MODEL proj.test_openaai_unsupported_model_model
            PREDICT answer
            USING
                engine='openai_engine',
                model_name='this-model-does-not-exist',
                prompt_template='dummy_prompt_template';
            """
        )
        with pytest.raises(Exception) as excinfo:
            self.wait_predictor("proj", "test_openaai_unsupported_model_model")

        assert "Invalid model name." in str(excinfo.value)

    def test_single_full_flow_in_default_mode_runs_no_errors_with_question_column(self):
        """
        Test the full flow in default mode with a question column for a single prediction.
        """
        self.run_sql(
            f"""
            CREATE MODEL proj.test_openai_single_full_flow_default_mode_question_column
            PREDICT answer
            USING
                engine='openai_engine';
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_default_mode_question_column")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_default_mode_question_column
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_full_flow_in_default_mode_runs_no_errors_with_question_column(self, mock_handler):
        """
        Test the full flow in default mode with a question column for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
            CREATE MODEL proj.test_openai_bulk_full_flow_default_mode_question_column
            PREDICT answer
            USING
                engine='openai_engine',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_default_mode_question_column")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_full_flow_default_mode_question_column as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()

    def test_single_full_flow_in_default_mode_runs_no_errors_with_prompt_template(self):
        """
        Test the full flow in default mode with a prompt template for a single prediction.
        """
        self.run_sql(
            f"""
            CREATE MODEL proj.test_openai_single_full_flow_default_mode_prompt_template
            PREDICT answer
            USING
                engine='openai_engine',
                prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}';
            """
        )

        self.wait_predictor("proj", "test_openai_single_full_flow_default_mode_prompt_template")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_openai_single_full_flow_default_mode_prompt_template
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "boom!" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_full_flow_in_default_mode_runs_no_errors_with_prompt_template(self, mock_handler):
        """
        Test the full flow in default mode with a prompt template for bulk predictions.
        """
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
            CREATE MODEL proj.test_openai_bulk_full_flow_default_mode_prompt_template
            PREDICT answer
            USING
                engine='openai_engine',
                prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}';
            """
        )

        self.wait_predictor("proj", "test_openai_bulk_full_flow_default_mode_prompt_template")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_openai_bulk_full_flow_default_mode_prompt_template as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "boom!" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()
        assert "boom!" in result_df["answer"].iloc[1].lower()


if __name__ == "__main__":
    pytest.main([__file__])
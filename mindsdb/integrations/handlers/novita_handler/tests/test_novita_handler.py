import os
import pytest
import pandas as pd
from unittest.mock import patch

from tests.unit.ml_handlers.base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get("MDB_TEST_NOVITA_API_KEY") is None, reason="Missing API key!")
class TestNovita(BaseMLAPITest):
    """
    Integration tests for the Novita handler.
    """

    def setup_method(self):
        """
        Setup test environment by creating a project and a Novita engine.
        """
        super().setup_method()
        self.run_sql("CREATE DATABASE proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE novita_engine
            FROM novita
            USING
            novita_api_key = '{self.get_api_key("MDB_TEST_NOVITA_API_KEY")}';
            """
        )

    def test_create_model_with_question_column_runs_no_errors(self):
        """
        Test creating a model with a question column for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_novita_question_column
            PREDICT answer
            USING
                engine='novita_engine',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_novita_question_column")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_novita_question_column
            WHERE question='What is the capital of Sweden?'
            """
        )
        assert result_df["answer"].iloc[0] is not None

    def test_create_model_with_prompt_template_runs_no_errors(self):
        """
        Test creating a model with a prompt template for a single prediction.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_novita_prompt_template
            PREDICT answer
            USING
                engine='novita_engine',
                prompt_template='Answer this question: {{question}}';
            """
        )

        self.wait_predictor("proj", "test_novita_prompt_template")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_novita_prompt_template
            WHERE question='What is the capital of France?'
            """
        )
        assert result_df["answer"].iloc[0] is not None

    def test_create_model_with_custom_model_name_runs_no_errors(self):
        """
        Test creating a model with a specific Novita model name.
        """
        self.run_sql(
            """
            CREATE MODEL proj.test_novita_custom_model
            PREDICT answer
            USING
                engine='novita_engine',
                model_name='moonshotai/kimi-k2.5',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_novita_custom_model")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_novita_custom_model
            WHERE question='What is 2+2?'
            """
        )
        assert result_df["answer"].iloc[0] is not None

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_prediction_runs_no_errors(self, mock_handler):
        """
        Test bulk predictions with the Novita handler.
        """
        df = pd.DataFrame.from_dict({"question": ["What is the capital of Sweden?", "What is the capital of Norway?"]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_novita_bulk
            PREDICT answer
            USING
                engine='novita_engine',
                question_column='question';
            """
        )

        self.wait_predictor("proj", "test_novita_bulk")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_novita_bulk as p;
            """
        )
        assert result_df["answer"].iloc[0] is not None
        assert result_df["answer"].iloc[1] is not None


if __name__ == "__main__":
    pytest.main([__file__])

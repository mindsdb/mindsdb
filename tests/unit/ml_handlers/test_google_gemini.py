import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest

GEMINI_API_KEY = os.environ.get('GOOGLE_GENAI_API_KEY')


@pytest.mark.skipif(GEMINI_API_KEY is None, reason='Missing API key!')
class TestGeminiHandler(BaseMLAPITest):
    """Test Class for Google Gemini (Bard) API handler"""

    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    def test_invalid_model_parameter(self):
        """Test for invalid Gemini model parameter"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_google_invalid_model
            PREDICT answer
            USING
                engine='google_gemini',
                column='question',
                model='non-existing-gemini-model',
                api_key='{GEMINI_API_KEY}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_google_invalid_model")

    @pytest.mark.skip(reason="This test is failing as no error is being thrown")
    def test_unknown_model_argument(self):
        """Test for unknown argument when creating Gemini model"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_google_unknown_arg
            PREDICT answer
            USING
                engine='google',
                column='question',
                api_key='{GEMINI_API_KEY}',
                evidently_wrong_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_google_unknown_arg")

    def test_single_qa(self):
        """Test for single question/answer pair"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_google_single_qa
            PREDICT answer
            USING
                engine='google_gemini',
                column='question',
                api_key='{GEMINI_API_KEY}';
            """
        )
        self.wait_predictor("proj", "test_google_single_qa")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_google_single_qa
            WHERE question = 'What is the capital of Sweden?';
        """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_qa(self, mock_handler):
        """Test for bulk question/answer pairs"""
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
           CREATE MODEL proj.test_google_bulk_qa
           PREDICT answer
           USING
               engine='google_gemini',
               column='question',
               api_key='{GEMINI_API_KEY}';
        """
        )
        self.wait_predictor("proj", "test_google_bulk_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_google_bulk_qa as p;
        """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()

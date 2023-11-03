import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('ANTHROPIC_API_KEY') is None, reason='Missing API key!')
class TestAnthropic(BaseMLAPITest):
    """Test Class for Anthropic Integration Testing"""

    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE anthropic
            FROM anthropic
            USING
            api_key = '{self.get_api_key('ANTHROPIC_API_KEY')}';
            """
        )

    def test_invalid_model_parameter(self):
        """Test for invalid Anthropic model parameter"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_anthropic_invalid_model
            PREDICT answer
            USING
                engine='anthropic',
                column='question',
                model='this-claude-does-not-exist',
                api_key='{self.get_api_key('ANTHROPIC_API_KEY')}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anthropic_invalid_model")

    def test_unknown_model_argument(self):
        """Test for unknown argument when creating a Anthropic model"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_anthropic_unknown_argument
            PREDICT answer
            USING
                engine='anthropic',
                column='question',
                api_key='{self.get_api_key('ANTHROPIC_API_KEY')}',
                evidently_wrong_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anthropic_unknown_argument")

    def test_single_qa(self):
        """Test for single question/answer pair"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_anthropic_single_qa
            PREDICT answer
            USING
                engine='anthropic',
                column='question',
                api_key='{self.get_api_key('ANTHROPIC_API_KEY')}';
            """
        )
        self.wait_predictor("proj", "test_anthropic_single_qa")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_anthropic_single_qa
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
           CREATE MODEL proj.test_anthropic_bulk_qa
           PREDICT answer
           USING
               engine='anthropic',
               column='question',
               api_key='{self.get_api_key('ANTHROPIC_API_KEY')}';
        """
        )
        self.wait_predictor("proj", "test_anthropic_bulk_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_anthropic_bulk_qa as p;
        """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()

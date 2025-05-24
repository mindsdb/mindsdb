import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('LEONARDO_API_KEY') is None, reason='Missing API key!')
class TestLeonardoAI(BaseMLAPITest):
    """Test Class for LeonardoAI Integration Testing."""

    @staticmethod
    def get_api_key():
        """Retrieve Leonardo API key from environment variables."""
        return os.environ.get('LEONARDO_API_KEY')

    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE leo_engine
            FROM leonardo_ai
            USING
            api_key = '{self.get_api_key('LEONARDO_API_KEY')}';
            """
        )

    def test_invalid_model_parameter(self):
        """Test for invalid Leonardo model parameter"""
        self.run_sql(
            """
            CREATE MODEL proj.test_leonardo_invalid_model
            PREDICT url
            USING
            engine = 'leo_engine',
            model = 'invalid-model',
            prompt_template = '{{text}}, 8K | highly detailed realistic 3d oil painting style cyberpunk by MAD DOG JONES combined with Van Gogh  |  cinematic lighting | happy colors';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_leonardo_invalid_model")

    def test_unknown_model_argument(self):
        """Test for unknown argument when creating a Leonardo model"""
        self.run_sql(
            """
            CREATE MODEL proj.test_leonardo_unknown_argument
            PREDICT url
            USING
            engine = 'leo_engine',
            model = 'invalid-model',
            prompt_template = '{{text}}, 8K | highly detailed realistic 3d oil painting style cyberpunk by MAD DOG JONES combined with Van Gogh  |  cinematic lighting | happy colors',
            evidently_wrong_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_leonardo_unknown_argument")

    def test_single_qa(self):
        """Test for single image generation"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_leonardo_single_qa
            PREDICT url
            USING
            engine = 'leo_engine',
            model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3',
            api_key = '{self.get_api_key('LEONARDO_API_KEY')}',
            prompt_template = '{{text}}, 8K | highly detailed realistic 3d oil painting style cyberpunk by MAD DOG JONES combined with Van Gogh  |  cinematic lighting | happy colors';
            """
        )
        self.wait_predictor("proj", "test_leonardo_single_qa")

        result_df = self.run_sql(
            """
            SELECT *
            FROM mindsdb.leo
            WHERE text = 'Generate a random ANIME picture';
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_qa(self, mock_handler):
        """Test for bulk image processing"""
        df = pd.DataFrame.from_dict({"prompt": [
            "Abstract artwork with vibrant colors and dynamic shapes. Imagine a world where sound is visible, and each element in the image represents a different genre of music.",
            "surreal landscape where mountains are made of candy, and rivers flow with liquid gold."
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
            CREATE MODEL proj.test_leonardo_bulk_qa
            PREDICT url
            USING
            engine = 'leo_engine',
            model = '6bef9f1b-29cb-40c7-b9df-32b51c1f67d3',
            api_key = '{self.get_api_key('LEONARDO_API_KEY')}',
            prompt_template = '{{text}}, 8K | highly detailed realistic 3d oil painting style cyberpunk by MAD DOG JONES combined with Van Gogh  |  cinematic lighting | happy colors';
            """
        )
        self.wait_predictor("proj", "test_leonardo_bulk_qa")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_leonardo_bulk_qa as p;
            """
        )
        assert "stockholm" in result_df["answer"].iloc[0].lower()
        assert "venus" in result_df["answer"].iloc[1].lower()

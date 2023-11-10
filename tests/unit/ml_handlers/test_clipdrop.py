import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('CLIPDROP_API_KEY') is None, reason='Missing API key!')
class TestClipdrop(BaseMLAPITest):
    """Test Class for Clipdrop Integration Testing"""

    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE clipdrop_engine
            FROM clipdrop
            USING
            api_key = '{self.get_api_key('CLIPDROP_API_KEY')}';
            """
        )

    def test_missing_task_argument(self):
        """Test for unknown argument when creating a clidrop model"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_clipdrop_invalid_model
            PREDICT answer
            USING
                engine='clipdrop_engine',
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('CLIPDROP_API_KEY')}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_missing_task_argument")

    def test_unknown_task_argument(self):
        """Test for unknown argument when creating a clipdrop model"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_clipdrop_invalid_model
            PREDICT answer
            USING
                engine='clipdrop_engine',
                task = "unknown-task",
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('CLIPDROP_API_KEY')}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unknown_task_argument")

    def test_text_image_single(self):
        """Test for single text"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_clipdrop_t2i_single
            PREDICT answer
            USING
                engine='clipdrop_engine',
                task = "text_to_image",
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('CLIPDROP_API_KEY')}';
            """
        )
        self.wait_predictor("proj", "test_clipdrop_t2i_single")

        result_df = self.run_sql(
            """
            SELECT *
            FROM proj.test_clipdrop_t2i_single
            WHERE text = 'A blue lagoon';
        """
        )
        assert result_df["answer"].size == 1

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_bulk_text(self, mock_handler):
        """Test for bulk question/answer pairs"""
        df = pd.DataFrame.from_dict({"text": [
            "A black swan",
            "A pink unicorn"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        self.run_sql(
            f"""
           CREATE MODEL proj.test_clipdrop_bulk_text
           PREDICT answer
           USING
               engine='clipdrop_engine',
                task = "text_to_image",
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('CLIPDROP_API_KEY')}';
        """
        )
        self.wait_predictor("proj", "test_clipdrop_bulk_text")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_clipdrop_bulk_text as p;
        """
        )
        assert result_df["answer"].size == 2

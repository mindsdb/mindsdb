import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('STABILITY_API_KEY') is None, reason='Missing Stability API key!')
class TestStabilityAI(BaseMLAPITest):
    """Test Class for Stability Integration Testing"""

    def setup_method(self):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE stability_engine
            FROM stabilityai
            USING
            api_key = '{self.get_api_key('STABILITY_API_KEY')}';
            """
        )

    def test_invalid_model_parameter(self):
        """Test for invalid Stability model parameter"""
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                CREATE MODEL proj.test_stability_invalid_model
                PREDICT answer
                USING
                    engine='stability_engine',
                    engine_id = 'this-engine-does-not-exist',
                    task = "text-to-image",
                    local_directory_path = "tests/unit/ml_handlers/data",
                    api_key='{self.get_api_key('STABILITY_API_KEY')}';
                """
            )
            self.wait_predictor("proj", "test_stability_invalid_model")

    def test_missing_task_argument(self):
        """Test for unknown argument when creating a stability model"""
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                CREATE MODEL proj.test_stability_invalid_model
                PREDICT answer
                USING
                    engine='stability_engine',
                    local_directory_path = "tests/unit/ml_handlers/data",
                    api_key='{self.get_api_key('STABILITY_API_KEY')}';
                """
            )
            self.wait_predictor("proj", "test_missing_task_argument")

    def test_unknown_task_argument(self):
        """Test for unknown argument when creating a stability model"""
        with pytest.raises(Exception):
            self.run_sql(
                f"""
                CREATE MODEL proj.test_stability_invalid_model
                PREDICT answer
                USING
                    engine='stability_engine',
                    task = "unknown-task",
                    local_directory_path = "tests/unit/ml_handlers/data",
                    api_key='{self.get_api_key('STABILITY_API_KEY')}';
                """
            )
            self.wait_predictor("proj", "test_unknown_task_argument")

    def test_text_image_single(self):
        """Test for single text"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_stability_t2i_single
            PREDICT answer
            USING
                engine='stability_engine',
                task = "text-to-image",
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('STABILITY_API_KEY')}';
            """
        )
        self.wait_predictor("proj", "test_stability_t2i_single")

        result_df = self.run_sql(
            """
            SELECT *
            FROM proj.test_stability_t2i_single
            WHERE text = 'A blue lagoon';
        """
        )
        assert result_df["answer"].size == 1

    def test_bulk_text(self):
        """Test for bulk question/answer pairs"""
        df = pd.DataFrame.from_dict({"text": [
            "A black swan",
            "A pink unicorn"
        ]})
        self.set_data('df', df)

        self.run_sql(
            f"""
           CREATE MODEL proj.test_stability_bulk_text
           PREDICT answer
           USING
               engine='stability_engine',
                task = "text-to-image",
                local_directory_path = "tests/unit/ml_handlers/data",
                api_key='{self.get_api_key('STABILITY_API_KEY')}';
        """
        )
        self.wait_predictor("proj", "test_stability_bulk_text")

        result_df = self.run_sql(
            """
            SELECT p.answer
            FROM dummy_data.df as t
            JOIN proj.test_stability_bulk_text as p;
        """
        )
        assert result_df["answer"].size == 2

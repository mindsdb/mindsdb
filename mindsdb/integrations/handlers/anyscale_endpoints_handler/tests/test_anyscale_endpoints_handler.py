import os
import pytest
import pandas as pd
from unittest.mock import patch

from tests.unit.ml_handlers.base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('MDB_TEST_ANYSCALE_ENDPOINTS_API_KEY') is None, reason='Missing API key!')
class TestAnyscaleEndpoints(BaseMLAPITest):
    """
    Integration tests for Anyscale Endpoints AI engine.
    """

    def setup_method(self):
        """
        Setup test environment by creating a project and a Anyscale Endpoints engine.
        """

        super().setup_method()
        self.run_sql("CREATE DATABASE proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE anyscale_endpoints_engine
            FROM anyscale_endpoints
            USING
            anyscale_endpoints_api_key = '{self.get_api_key('MDB_TEST_ANYSCALE_ENDPOINTS_API_KEY')}';
            """
        )

    def test_create_model_raises_exception_with_invalid_model_parameter(self):
        """
        Test for invalid parameter during model creation.
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_anyscale_invalid_parameter_model
            PREDICT answer
            USING
                engine='anyscale_endpoints_engine',
                model_name='this-model-does-not-exist',
                prompt_template='dummy_prompt_template';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anyscale_invalid_parameter_model")

    def test_create_model_raises_exception_with_unknown_model_argument(self):
        """
        Test for unknown argument during model creation.
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_anyscale_unknown_argument_model
            PREDICT answer
            USING
                engine='anyscale_endpoints_engine',
                prompt_template='dummy_prompt_template',
                evidently_wrong_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anyscale_unknown_argument_model")

    def test_create_model_raises_exception_with_invalid_operation_mode(self):
        """
        Test for invalid operation mode during model creation.
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_anyscale_invalid_operation_mode
            PREDICT answer
            USING
                engine='anyscale_endpoints_engine',
                prompt_template='dummy_prompt_template',
                mode='invalid_mode';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anyscale_invalid_operation_mode")

    def test_select_runs_no_errors_on_completion_sentiment_analysis_single(self):
        """
        Test for a valid response to a sentiment analysis task (completion).
        """

        self.run_sql(
            """
            CREATE MODEL proj.test_anyscale_single_sa
            PREDICT sentiment
            USING
                engine='anyscale_endpoints_engine',
                model_name = 'mistralai/Mistral-7B-Instruct-v0.1',
                prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
            """
        )
        self.wait_predictor("proj", "test_anyscale_single_sa")

        result_df = self.run_sql(
            """
            SELECT sentiment
            FROM proj.test_anyscale_single_sa
            WHERE text = 'I love machine learning!';
            """
        )

        assert "positive" in result_df["sentiment"].iloc[0].lower()

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_select_runs_no_errors_on_completion_sentiment_analysis_bulk(self, mock_postgres_handler):
        """
        Test for valid reponses to bulk questions in a sentiment analysis task (completion).
        """

        df = pd.DataFrame.from_dict({"text": [
            "I love machine learning!",
            "I hate slow internet connections!"
        ]})
        self.set_handler(mock_postgres_handler, name="pg", tables={"df": df})

        self.run_sql(
            """
            CREATE MODEL proj.test_anyscale_bulk_sa
            PREDICT sentiment
            USING
                engine='anyscale_endpoints_engine',
                model_name = 'mistralai/Mistral-7B-Instruct-v0.1',
                prompt_template = 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}';
            """
        )
        self.wait_predictor("proj", "test_anyscale_bulk_sa")

        result_df = self.run_sql(
            """
            SELECT p.sentiment
            FROM pg.df as t
            JOIN proj.test_anyscale_bulk_sa as p;
        """
        )

        assert "positive" in result_df["sentiment"].iloc[0].lower()
        assert "negative" in result_df["sentiment"].iloc[1].lower()


if __name__ == '__main__':
    pytest.main([__file__])
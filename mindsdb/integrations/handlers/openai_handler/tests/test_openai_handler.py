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


if __name__ == "__main__":
    pytest.main([__file__])
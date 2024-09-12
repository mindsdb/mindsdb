import os
import pytest
import pandas as pd
from unittest.mock import patch

from .base_ml_test import BaseMLAPITest


# @pytest.mark.skipif(os.environ.get('UNIFY_API_KEY') is None, reason='Missing API key!')
class TestUnify(BaseMLAPITest):
    """
    Integration tests for the Unify handler.
    """

    def setup_method(self):
        """
        Setup test environment, creating a project
        """
        super().setup_method()
        self.run_sql("CREATE database proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE unify_engine
            FROM unify
            USING
            unify_api_key = '{self.get_api_key('UNIFY_API_KEY')}';
            """
        )

    def test_unsupported_model(self):
        """Test for unsupported model in Unify"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_unify_unsupported_model
            PREDICT output
            USING
                engine='unify-engine',
                column='text',
                model='this-model-does-not-exist',
                api_key='{self.get_api_key('ANTHROPIC_API_KEY')}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unsupported_model")

    def test_unknown_model_argument(self):
        """Test for unknown argument when creating a Unify model"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_unify_unknown_argument
            PREDICT output
            USING
                engine='unify-engine',
                column='text',
                api_key='{self.get_api_key('UNIFY_API_KEY')}',
                unknown_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_unify_unknown_argument")
            
    def test_correct_flow_in_default_mode(self):
        """Test for correct flow: model to give answer to given instruction"""
        self.run_sql(
            f"""
            CREATE MODEL proj.test_unify_correct_flow
            PREDICT output
            USING
                engine='unify_engine',
                model = 'llama-3-8b-chat',
                provider = 'together-ai',
                column = 'text',
                api_key='{self.get_api_key('UNIFY_API_KEY')}';
            """
        )
        self.wait_predictor("proj", "test_unify_correct_flow")

        result_df = self.run_sql(
        """
            SELECT text, output
            FROM proj.test_unify_correct_flow
            WHERE text = 'Hello';
        """
        )
        assert "Hello" in result_df["answer"].iloc[0].lower()
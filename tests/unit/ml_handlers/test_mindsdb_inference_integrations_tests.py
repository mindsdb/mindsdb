import os
import pytest

from .base_ml_test import BaseMLAPITest


@pytest.mark.skipif(os.environ.get('MDB_TEST_MDB_INFERENCE_API_KEY') is None, reason='Missing API key!')
class TestMindsDBInference(BaseMLAPITest):
    """
    Integration tests for MindsDB Inference engine.
    """

    # TODO: Should random names be generated for the project, model etc.?
    # TODO: Are the resources created being cleaned up after the test?

    def setup_method(self):
        """
        Setup test environment by creating a project and a MindsDB Inference engine.
        """

        super().setup_method()
        self.run_sql("CREATE DATABASE proj")
        self.run_sql(
            f"""
            CREATE ML_ENGINE mindsdb_inference_engine
            FROM mindsdb_inference
            USING
            mindsdb_inference_api_key = '{self.get_api_key('MDB_TEST_MDB_INFERENCE_API_KEY')}';
            """
        )

    def test_create_model_raises_exception_with_invalid_model_parameter(self):
        """
        Test for invalid parameter during model creation.
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_mdb_inference_invalid_parameter_model
            PREDICT answer
            USING
                engine='mindsdb_inference_engine',
                model_name='this-model-does-not-exist',
                prompt_template='dummy_prompt_template',
                mindsdb_inference_api_key='{self.get_api_key('MDB_TEST_MDB_INFERENCE_API_KEY')}';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_mdb_inference_invalid_model")

    def test_create_model_raises_exception_with_unknown_model_argument(self):
        """
        Test for unknown argument during model creation.
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_mdb_inference_unknown_argument_model
            PREDICT answer
            USING
                engine='mindsdb_inference_engine',
                prompt_template='dummy_prompt_template',
                mindsdb_inference_api_key='{self.get_api_key('MDB_TEST_MDB_INFERENCE_API_KEY')}',
                evidently_wrong_argument='wrong value';
            """
        )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_mdb_inference_unknown_argument_model")

    def test_select_runs_no_errors_on_chat_completion_question_answering_single(self):
        """
        Test for a valid answer to a question answering task (chat completion).
        """

        self.run_sql(
            f"""
            CREATE MODEL proj.test_mdb_inference_single_qa
            PREDICT answer
            USING
                engine='mindsdb_inference_engine',
                question_column='question',
                mindsdb_inference_api_key='{self.get_api_key('MDB_TEST_MDB_INFERENCE_API_KEY')}';
            """
        )
        self.wait_predictor("proj", "test_mdb_inference_single_qa")

        result_df = self.run_sql(
            """
            SELECT answer
            FROM proj.test_mdb_inference_single_qa
            WHERE question = 'What is the capital of Sweden?';
            """
        )

        assert "stockholm" in result_df["answer"].iloc[0].lower()


if __name__ == "__main__":
    pytest.main()
import os
import time
import pytest
import contextlib
import pandas as pd
from unittest.mock import patch
from mindsdb_sql import parse_sql
from mindsdb.integrations.handlers.anyscale_endpoints_handler.anyscale_endpoints_handler import AnyscaleEndpointsHandler
from ..executor_test_base import BaseExecutorTest


# @pytest.mark.skipif(os.environ.get('ANYSCALE_ENDPOINTS_API_KEY') is None, reason='Missing API key!')
class TestAnyscaleEndpoints(BaseExecutorTest):
    """
        Test class for Anyscale Endpoints Integration Testing.
        It follows tests for OpenAI's handler closely as the Anyscale handler inherits from it.
        Notably, no assertions are done on the as the 7B model is not very good at this task.
    """

    @staticmethod
    @contextlib.contextmanager
    def _use_anyscale_api_key(openai_key='OPENAI_API_KEY'):
        """ Temporarily updates the API key env var for OpenAI with the Anyscale one. """
        old_key = os.environ.get(openai_key)
        os.environ[openai_key] = os.environ.get('ANYSCALE_ENDPOINTS_API_KEY')
        try:
            yield os.environ[openai_key]  # enter
        finally:
            os.environ[openai_key] = old_key  # exit

    def setup_method(self, method):
        """Setup test environment, creating a project"""
        super().setup_method()
        self.run_sql("create database proj")

    def wait_predictor(self, project, name, timeout=100):
        """
        Wait for the predictor to be created,
        raising an exception if predictor creation fails or exceeds timeout
        """
        for attempt in range(timeout):
            ret = self.run_sql(f"select * from {project}.models where name='{name}'")
            if not ret.empty:
                status = ret["STATUS"][0]
                if status == "complete":
                    return
                elif status == "error":
                    raise RuntimeError("Predictor failed", ret["ERROR"][0])
            time.sleep(0.5)
        raise RuntimeError("Predictor wasn't created")

    def run_sql(self, sql):
        """Execute SQL and return a DataFrame, raising an AssertionError if an error occurs"""
        ret = self.command_executor.execute_command(parse_sql(sql, dialect="mindsdb"))
        assert ret.error_code is None, f"SQL execution failed with error: {ret.error_code}"
        if ret.data is not None:
            columns = [col.alias if col.alias else col.name for col in ret.columns]
            return pd.DataFrame(ret.data, columns=columns)

    def test_1_missing_required_keys(self):
        """Test for missing required keys"""
        with pytest.raises(Exception):
            with self._use_anyscale_api_key() as api_key:
                self.run_sql(
                    f"""
                    create model proj.test_anyscale_missing_required_keys
                    predict answer
                    using
                      engine='anyscale_endpoints',
                      api_key='{api_key}';
                    """
                )

    def test_2_invalid_anyscale_name_parameter(self):
        """Test for invalid OpenAI model name parameter"""
        with self._use_anyscale_api_key() as api_key:
            self.run_sql(
                f"""
                create model proj.test_anyscale_nonexistant_model
                predict answer
                using
                  engine='anyscale_endpoints',
                  question_column='question',
                  model_name='this-model-is-not-supported',
                  api_key='{api_key}';
                """
            )
        with pytest.raises(Exception):
            self.wait_predictor("proj", "test_anyscale_nonexistant_model")

    def test_3_unknown_arguments(self):
        """Test for unknown arguments"""
        with pytest.raises(Exception):
            with self._use_anyscale_api_key() as api_key:
                self.run_sql(
                    f"""
                    create model proj.test_anyscale_unknown_arguments
                    predict answer
                    using
                      engine='anyscale_endpoints',
                      question_column='question',
                      api_key='{api_key}',
                      evidently_wrong_argument='wrong value';
                    """
                )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_4_qa_no_context(self, mock_handler):
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        with self._use_anyscale_api_key() as api_key:
            self.run_sql(
                f"""
               create model proj.test_anyscale_qa_no_context
               predict answer
               using
                 engine='anyscale_endpoints',
                 question_column='question',
                 api_key='{api_key}';
            """
            )
        self.wait_predictor("proj", "test_anyscale_qa_no_context")

        self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_anyscale_qa_no_context as p
            WHERE question='What is the capital of Sweden?'
        """
        )

        self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_anyscale_qa_no_context as p;
        """
        )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_5_qa_context(self, mock_handler):
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ], "context": ['Add "Boom!" to the end of the answer.', 'Add "Boom!" to the end of the answer.']})
        self.set_handler(mock_handler, name="pg", tables={"df": df})

        with self._use_anyscale_api_key() as api_key:
            self.run_sql(
                f"""
               create model proj.test_anyscale_qa_context
               predict answer
               using
                 engine='anyscale_endpoints',
                 question_column='question',
                 context_column='context',
                 api_key='{api_key}';
            """
            )
        self.wait_predictor("proj", "test_anyscale_qa_context")

        self.run_sql(
            """
            SELECT p.answer
            FROM proj.test_anyscale_qa_context as p
            WHERE
            question='What is the capital of Sweden?' AND
            context='Add "Boom!" to the end of the answer.'
        """
        )

        self.run_sql(
            """
            SELECT p.answer
            FROM pg.df as t
            JOIN proj.test_anyscale_qa_context as p;
        """
        )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_6_prompt_template(self, mock_handler):
        df = pd.DataFrame.from_dict({"question": [
            "What is the capital of Sweden?",
            "What is the second planet of the solar system?"
        ]})
        self.set_handler(mock_handler, name="pg", tables={"df": df})
        with self._use_anyscale_api_key() as api_key:
            self.run_sql(
                f"""
               create model proj.test_anyscale_prompt_template
               predict completion
               using
                 engine='anyscale_endpoints',
                 prompt_template='Answer this question and add "Boom!" to the end of the answer: {{{{question}}}}',
                 api_key='{api_key}';
            """
            )
        self.wait_predictor("proj", "test_anyscale_prompt_template")

        self.run_sql(
            """
            SELECT p.completion
            FROM proj.test_anyscale_prompt_template as p
            WHERE
            question='What is the capital of Sweden?';
        """
        )

        self.run_sql(
            """
            SELECT p.completion
            FROM pg.df as t
            JOIN proj.test_anyscale_prompt_template as p;
        """
        )

    @patch("mindsdb.integrations.handlers.postgres_handler.Handler")
    def test_7_bulk_normal_completion(self, mock_handler):
        """Tests normal completions (e.g. text-davinci-003) with bulk joins that are larger than the max batch_size"""
        class MockHandlerStorage:
            def json_get(self, key):
                return {'ft-suffix': {'ft-suffix': '$'}}[key]  # finetuning suffix, irrelevant for this test but needed for init  # noqa

        # create project
        handler = AnyscaleEndpointsHandler(
            model_storage=None,  # the storage does not matter for this test
            engine_storage=MockHandlerStorage()  # nor does this, but we do need to pass some object due to the init procedure  # noqa
        )
        N = 1 + handler.max_batch_size  # get N larger than default batch size
        df = pd.DataFrame.from_dict({"input": ["I feel happy!"] * N})
        self.set_handler(mock_handler, name="pg", tables={"df": df})
        with self._use_anyscale_api_key() as api_key:
            self.run_sql(
                f"""
               create model proj.test_anyscale_bulk_normal_completion
               predict completion
               using
                 engine='anyscale_endpoints',
                 prompt_template='What is the sentiment of the following phrase? Answer either "positive" or "negative": {{{{input}}}}',
                 api_key='{api_key}';
            """  # noqa
            )
        self.wait_predictor("proj", "test_anyscale_bulk_normal_completion")

        self.run_sql(
            """
            SELECT p.completion
            FROM pg.df as t
            JOIN proj.test_anyscale_bulk_normal_completion as p;
        """
        )

    # TODO: finetune test

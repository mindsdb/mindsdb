import pandas
import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from mindsdb.integrations.handlers.openai_handler.openai_handler import OpenAIHandler


class TestOpenAI(unittest.TestCase):
    """
    Unit tests for the OpenAI handler.
    """

    dummy_connection_data = OrderedDict(
        openai_api_key='dummy_api_key',
    )

    def setUp(self):
        # Mock model storage and engine storage
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()

        # Define a return value for the `get_connection_args` method of the mock engine storage
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data

        # Assign mock engine storage to instance variable for create validation tests
        self.mock_engine_storage = mock_engine_storage

        self.handler = OpenAIHandler(mock_model_storage, mock_engine_storage, connection_data={'connection_data': self.dummy_connection_data})

    def test_create_validation_raises_exception_without_using_clause(self):
        """
        Test if model creation raises an exception without a USING clause.
        """

        with self.assertRaisesRegex(Exception, "OpenAI engine requires a USING clause! Refer to its documentation for more details."):
            self.handler.create_validation('target', args={}, handler_storage=None)

    def test_create_validation_raises_exception_without_required_parameters(self):
        """
        Test if model creation raises an exception without required parameters.
        """

        with self.assertRaisesRegex(Exception, "One of `question_column`, `prompt_template` or `json_struct` is required for this engine."):
            self.handler.create_validation('target', args={"using": {}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_raises_exception_with_invalid_parameter_combinations(self):
        """
        Test if model creation raises an exception with invalid parameter combinations.
        """

        with self.assertRaisesRegex(Exception, "^Please provide one of"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template', 'question_column': 'question'}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_raises_exception_with_unknown_arguments(self):
        """
        Test if model creation raises an exception with unknown arguments.
        """

        with self.assertRaisesRegex(Exception, "^Unknown arguments:"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template', 'unknown_arg': 'unknown_arg'}}, handler_storage=self.mock_engine_storage)

    def test_create_validation_raises_exception_with_invalid_api_key(self):
        """
        Test if model creation raises an exception with an invalid API key.
        """

        with self.assertRaisesRegex(Exception, "Invalid api key"):
            self.handler.create_validation('target', args={"using": {'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.mock_engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_validation_runs_no_errors_with_valid_arguments(self, mock_openai):
        """
        Test if model creation is validated correctly with valid arguments.
        """

        # Mock the models.retrieve method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.models.retrieve.return_value = MagicMock()

        mock_openai.return_value = mock_openai_client

        self.handler.create_validation('target', args={'using': {'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.mock_engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_raises_exception_with_invalid_mode(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation raises an exception with an invalid mode.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        with self.assertRaisesRegex(Exception, "^Invalid operation mode."):
            self.handler.create('dummy_target', args={'using': {'prompt_template': 'dummy_prompt_template', 'mode': 'dummy_mode'}})

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_raises_exception_with_unsupported_model(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation raises an exception with an invalid model name.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        with self.assertRaisesRegex(Exception, "^Invalid model name."):
            self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_unsupported_model_name', 'prompt_template': 'dummy_prompt_template'}})

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_runs_no_errors_with_valid_arguments(self, mock_openai_handler_openai_client, mock_openai_helpers_openai_client):
        """
        Test if model creation runs without errors with valid arguments.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        self.handler.create('dummy_target', args={'using': {'prompt_template': 'dummy_prompt_template'}})

    def test_predict_raises_exception_with_invalid_mode(self):
        """
        Test if model prediction raises an exception with an invalid mode.
        """

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "^Invalid operation mode."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'dummy_mode'}})

    def test_predict_raises_exception_on_embedding_mode_without_question_column(self):
        """
        Test if model prediction raises an exception in embedding mode without a question column.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'embedding',
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "Embedding mode needs a question_column"):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'embedding'}})

    def test_predict_raises_exception_on_image_mode_without_question_column_or_prompt_template(self):
        """
        Test if model prediction raises an exception in image mode without a question column or prompt template.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'image',
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "Image mode needs either `prompt_template` or `question_column`."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'image'}})

    def test_predict_raises_exception_on_default_mode_without_question_column_in_df(self):
        """
        Test if model prediction raises an exception in default mode without a question column in the DataFrame.
        """

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'mode': 'default',
            'question_column': 'question'
        }

        # Create a dummy DataFrame
        df = pandas.DataFrame()

        with self.assertRaisesRegex(Exception, "Question column not found in the DataFrame."):
            self.handler.predict(df=df, args={'predict_params': {'mode': 'default'}})


if __name__ == '__main__':
    unittest.main()
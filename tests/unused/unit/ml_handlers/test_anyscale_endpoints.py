import pandas
import unittest
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from mindsdb.integrations.handlers.anyscale_endpoints_handler.anyscale_endpoints_handler import AnyscaleEndpointsHandler


class TestAnyscaleEndpoints(unittest.TestCase):
    """
    Unit tests for the Anyscale Endpoints handler.
    """

    dummy_connection_data = OrderedDict(
        anyscale_endpoints_api_key='dummy_api_key',
    )

    def setUp(self):
        # Mock model storage and engine storage
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()

        # Define a return value for the `get_connection_args` method of the mock engine storage
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data

        # Assign mock engine storage to instance variable for create validation tests
        self.mock_engine_storage = mock_engine_storage

        self.handler = AnyscaleEndpointsHandler(mock_model_storage, mock_engine_storage, connection_data={'connection_data': self.dummy_connection_data})

    def test_create_validation_raises_exception_without_using_clause(self):
        """
        Test if model creation raises an exception without a USING clause.
        """

        with self.assertRaisesRegex(Exception, "Anyscale Endpoints engine requires a USING clause! Refer to its documentation for more details."):
            self.handler.create_validation('target', args={}, handler_storage=None)

    def test_create_validation_raises_exception_with_invalid_api_key(self):
        """
        Test if model creation raises an exception with an invalid API key.
        """

        with self.assertRaises(Exception):
            self.handler.create_validation('target', args={"using": {}}, handler_storage=self.mock_engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_validation_with_valid_arguments(self, mock_openai):
        """
        Test if model creation is validated correctly with valid arguments.
        """

        # Mock the models.retrieve method of the OpenAI client
        mock_openai_client = MagicMock()
        mock_openai_client.models.retrieve.return_value = MagicMock()

        mock_openai.return_value = mock_openai_client

        self.handler.create_validation('target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.mock_engine_storage)

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
            self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template', 'mode': 'dummy_mode'}})

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
        Test if model creation with valid arguments runs without raising an Exception.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai_handler_openai_client.return_value.models.list.return_value = mock_models_list
        mock_openai_helpers_openai_client.return_value.models.list.return_value = mock_models_list

        self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template'}})

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_runs_no_errors_on_default_mode_prompt_completion(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for a sentiment analysis task.
        """

        mock_openai_client = MagicMock()

        # Mock the models.list method of the OpenAI client
        mock_models_list = [
            MagicMock(
                id='dummy_model_name',
            )
        ]

        mock_openai_client.models.list.return_value = mock_models_list

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'model_name': 'dummy_model_name',
            'prompt_template': 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}',
            'target': 'sentiment',
            'mode': 'default'
        }

        # Mock the completions.create method of the OpenAI client (for the OpenAI handler)
        mock_openai_client.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    text='Positive'
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['I love MindsDB!']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('sentiment' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'sentiment': ['Positive']}))

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_runs_no_errors_on_default_mode_question_answering(self, mock_openai_handler_openai_client):
        """
        Test if model prediction returns the expected result for a question answering task.
        """

        mock_openai_client = MagicMock()

        # Mock the models.list method of the OpenAI client
        mock_models_list = [
            MagicMock(
                id='dummy_model_name',
                rayllm_metadata={
                    'engine_config': {
                        'model_type': 'text-generation'
                    }
                }
            )
        ]

        mock_openai_client.models.list.return_value = mock_models_list

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'model_name': 'dummy_model_name',
            'question_column': 'question',
            'target': 'answer',
            'mode': 'default'
        }

        # Mock the chat.completions.create method of the OpenAI client (for the OpenAI handler)
        mock_openai_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='Sweden'
                    )
                )
            ]
        )

        mock_openai_handler_openai_client.return_value = mock_openai_client

        df = pandas.DataFrame({'question': ['Where is Stockholm located?']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('answer' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'answer': ['Sweden']}))


if __name__ == '__main__':
    unittest.main()

import pandas
import unittest
from collections import OrderedDict
from openai import AuthenticationError
from unittest.mock import patch, MagicMock

from mindsdb.integrations.handlers.mindsdb_inference.mindsdb_inference_handler import MindsDBInferenceHandler


class TestMindsDBInference(unittest.TestCase):
    """
    Unit tests for the MindsDB Inference handler.
    """

    dummy_connection_data = OrderedDict(
        mindsdb_inference_api_key='dummy_api_key',
    )

    def setUp(self):
        # Mock model storage and engine storage
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()

        # Define a return value for the `get_connection_args` method of the mock engine storage
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data

        self.handler = MindsDBInferenceHandler(mock_model_storage, mock_engine_storage, connection_data={'connection_data': self.dummy_connection_data})

    def test_create_validation_raises_exception_without_using_clause(self):
        """
        Test if model creation raises an exception without a USING clause.
        """

        with self.assertRaisesRegex(Exception, "MindsDB Inference engine requires a USING clause! Refer to its documentation for more details."):
            self.handler.create_validation('target', args={}, handler_storage=None)

    def test_create_validation_raises_exception_with_invalid_api_key(self):
        """
        Test if model creation raises an exception with an invalid API key.
        """

        with self.assertRaises(AuthenticationError):
            self.handler.create_validation('target', args={"using": {}}, handler_storage=self.handler.engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_create_validation_with_valid_arguments(self, mock_openai):
        """
        Test if model creation is validated correctly with valid arguments.
        """

        # Mock the chat.completions.create method of the OpenAI client (for the OpenAI handler)
        mock_openai_client = MagicMock()
        mock_openai_client.models.retrieve.return_value = MagicMock()

        mock_openai.return_value = mock_openai_client

        self.handler.create_validation('target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template'}}, handler_storage=self.handler.engine_storage)

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    def test_create_with_unsupported_mode(self, mock_openai):
        """
        Test if model creation runs without raising an Exception.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai.return_value.models.list.return_value = mock_models_list

        with self.assertRaisesRegex(Exception, "^Invalid operation mode."):
            self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template', 'mode': 'dummy_mode'}})

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    def test_create_with_valid_arguments(self, mock_openai):
        """
        Test if model creation runs without raising an Exception.
        """

        # Mock the models.list method of the OpenAI client
        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai.return_value.models.list.return_value = mock_models_list

        self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template'}})

    @patch('mindsdb.integrations.handlers.mindsdb_inference.mindsdb_inference_handler.openai.OpenAI')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAI')
    def test_predict_sentiment_analysis(self, mock_openai_openai_handler, mock_openai_mdb_inference_handler):
        """
        Test if model prediction returns the expected result for a sentiment analysis task.
        """

        # Mock the models.list method of the OpenAI client (for the MindsDB Inference handler)
        mock_supported_models = [MagicMock(id='mistral-7b')]
        mock_openai_mdb_inference_handler.return_value.models.list.return_value = mock_supported_models

        # Mock the json_get method of the model storage
        self.handler.model_storage.json_get.return_value = {
            'model_name': 'mistral-7b',
            'prompt_template': 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}',
            'target': 'sentiment',
            'mode': 'default',
            'api_base': 'https://llm.mdb.ai/'
        }

        # Mock the chat.completions.create method of the OpenAI client (for the OpenAI handler)
        mock_openai_client = MagicMock()
        mock_openai_client.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='positive'
                    )
                )
            ]
        )

        mock_openai_openai_handler.return_value = mock_openai_client

        df = pandas.DataFrame({'text': ['I love MindsDB!']})
        result = self.handler.predict(df, args={})

        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('sentiment' in result.columns)

        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'sentiment': ['positive']}))


if __name__ == '__main__':
    unittest.main()

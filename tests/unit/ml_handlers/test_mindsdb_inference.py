import pandas
import unittest
from unittest.mock import patch, MagicMock
from collections import OrderedDict

from mindsdb.integrations.handlers.mindsdb_inference.mindsdb_inference_handler import MindsDBInferenceHandler


class TestMindsDBInference(unittest.TestCase):

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

    def test_create_validation(self):
        """
        Test if `create_validation` method raises an Exception when `using` is not present in args.
        """
        with self.assertRaises(Exception):
            self.handler.create_validation('target', args={}, handler_storage=None)

    @patch('mindsdb.integrations.handlers.openai_handler.helpers.OpenAI')
    def test_create(self, mock_openai):
        """
        Test if `create` method returns a MindsDBInferenceHandler object.
        """

        mock_models_list = MagicMock()
        mock_models_list.data = [
            MagicMock(id='dummy_model_name')
        ]

        mock_openai.return_value.models.list.return_value = mock_models_list

        self.handler.create('dummy_target', args={'using': {'model_name': 'dummy_model_name', 'prompt_template': 'dummy_prompt_template'}})

    @patch('mindsdb.integrations.handlers.mindsdb_inference.mindsdb_inference_handler.MindsDBInferenceHandler._get_supported_models')
    @patch('mindsdb.integrations.handlers.openai_handler.openai_handler.OpenAIHandler._get_client')
    def test_predict_sentiment_analysis(self, mock_get_client, mock_get_models):
        """
        Test if `predict` method returns a DataFrame.
        """

        # Create a list of mock objects each with an `id` attribute
        mock_supported_models = [MagicMock(id='mistral-7b')]
        mock_get_models.return_value = mock_supported_models

        # Define a return value for the `json_get` method of the mock model storage
        self.handler.model_storage.json_get.return_value = {
            'model_name': 'mistral-7b',
            'prompt_template': 'Classify the sentiment of the following text as one of `positive`, `neutral` or `negative`: {{text}}',
            'target': 'sentiment',
            'mode': 'default',
            'api_base': 'https://llm.mdb.ai/'
        }

        df = pandas.DataFrame({'text': ['I love MindsDB!']})

        mock_openai = MagicMock()
        mock_openai.chat.completions.create.return_value = MagicMock(
            choices=[
                MagicMock(
                    message=MagicMock(
                        content='positive'
                    )
                )
            ]
        )

        mock_get_client.return_value = mock_openai

        result = self.handler.predict(df, args={})
        
        self.assertIsInstance(result, pandas.DataFrame)
        self.assertTrue('sentiment' in result.columns)
        
        pandas.testing.assert_frame_equal(result, pandas.DataFrame({'sentiment': ['positive']}))


if __name__ == '__main__':
    unittest.main()
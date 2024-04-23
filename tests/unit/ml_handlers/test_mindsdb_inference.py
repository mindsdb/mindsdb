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

        # Define a return value for the 'json_get' method of the mock model storage
        mock_model_storage.json_get.return_value = {}

        self.handler = MindsDBInferenceHandler(mock_model_storage, mock_engine_storage, connection_data={'connection_data': self.dummy_connection_data})

    def test_create_validation(self):
        """
        Test if `create_validation` method raises an Exception when `using` is not present in args.
        """
        with self.assertRaises(Exception):
            self.handler.create_validation('target', args={}, handler_storage=None)

    @patch('mindsdb.integrations.handlers.mindsdb_inference.mindsdb_inference_handler.MindsDBInferenceHandler._get_supported_models')
    def test_predict(self, mock_get_models):
        """
        Test if `predict` method returns a DataFrame.
        """

        # Create a list of mock objects each with an `id` attribute
        mock_supported_models = [MagicMock(id='model1'), MagicMock(id='model2'), MagicMock(id='model3')]
        mock_get_models.return_value = mock_supported_models

        df = MagicMock()
        result = self.handler.predict(df, args={})
        self.assertIsInstance(result, MagicMock)
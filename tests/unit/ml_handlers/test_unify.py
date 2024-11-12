import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
from collections import OrderedDict

from mindsdb.integrations.handlers.unify_handler.unify_handler import UnifyHandler  # Replace with the actual import path


class TestUnify(unittest.TestCase):
    """
    Unit tests for the Unify handler.
    """

    dummy_connection_data = OrderedDict(
        unify_api_key='dummy_api_key',
    )

    def setUp(self):
        # Mock model storage and engine storage
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()

        # Define a return value for the `get_connection_args` method of the mock engine storage
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data

        # Assign mock engine storage to instance variable for create validation tests
        self.mock_engine_storage = mock_engine_storage

        self.handler = UnifyHandler(mock_model_storage, mock_engine_storage)

    def test_create_without_using_clause_raises_exception(self):
        """
        Test if model creation raises an exception without a USING clause.
        """
        with self.assertRaisesRegex(Exception, "Unify engine requires a USING clause!"):
            self.handler.create('target', args={})

    def test_create_with_valid_arguments_runs_no_errors(self):
        """
        Test if model creation is validated correctly with valid arguments.
        """
        args = {
            'using': {
                'model': 'dummy_model',
                'provider': 'dummy_provider',
                'column': 'dummy_column'
            }
        }
        self.handler.create('target', args=args)
        self.assertTrue(self.handler.generative)
        self.handler.model_storage.json_set.assert_called_once_with('args', args)

    @patch('unify.utils.list_endpoints')
    @patch('unify.Unify')
    def test_predict_with_valid_arguments_runs_no_errors(self, mock_unify, mock_list_endpoints):
        """
        Test if model prediction returns the expected result with valid arguments.
        """
        # Mock the necessary methods and attributes
        self.handler.model_storage.json_get.return_value = {
            'using': {
                'model': 'dummy_model',
                'provider': 'dummy_provider',
                'column': 'input_text'
            },
            'target': 'output_text'
        }
        mock_list_endpoints.return_value = ['dummy_model@dummy_provider']

        mock_client = MagicMock()
        mock_client.generate.return_value = 'Generated text'
        mock_unify.return_value = mock_client

        # Create a dummy DataFrame
        df = pd.DataFrame({'input_text': ['Test input']})

        result = self.handler.predict(df)

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ['output_text'])
        self.assertEqual(result['output_text'][0], 'Generated text')

    def test_predict_with_missing_model_raises_exception(self):
        """
        Test if model prediction raises an exception when 'model' is missing from the USING clause.
        """
        self.handler.model_storage.json_get.return_value = {
            'using': {
                'provider': 'dummy_provider',
                'column': 'input_text'
            }
        }
        with self.assertRaisesRegex(Exception, "Unify requires an model parameter in the USING clause!"):
            self.handler.predict(pd.DataFrame())

    def test_predict_with_missing_provider_raises_exception(self):
        """
        Test if model prediction raises an exception when 'provider' is missing from the USING clause.
        """
        self.handler.model_storage.json_get.return_value = {
            'using': {
                'model': 'dummy_model',
                'column': 'input_text'
            }
        }
        with self.assertRaisesRegex(Exception, "Unify requires a provider parameter in the USING clause!"):
            self.handler.predict(pd.DataFrame())

    @patch('unify.utils.list_endpoints')
    def test_predict_with_unsupported_endpoint_raises_exception(self, mock_list_endpoints):
        """
        Test if model prediction raises an exception when the endpoint is not supported by Unify.
        """
        self.handler.model_storage.json_get.return_value = {
            'using': {
                'model': 'dummy_model',
                'provider': 'dummy_provider',
                'column': 'input_text'
            }
        }
        mock_list_endpoints.return_value = ['other_model@other_provider']

        with self.assertRaisesRegex(Exception, "The model, provider or their combination is not supported by Unify!"):
            self.handler.predict(pd.DataFrame())

    @patch('mindsdb.integrations.handlers.unify_handler.unify_handler.unify.utils.list_endpoints')
    @patch('mindsdb.integrations.handlers.unify_handler.unify_handler.get_api_key')
    def test_predict_with_missing_input_column_raises_exception(self, mock_get_api_key, mock_list_endpoints):
        # Mock the necessary methods and attributes
        mock_get_api_key.return_value = 'dummy_api_key'
        mock_list_endpoints.return_value = ['dummy_model@dummy_provider']

        self.handler.model_storage.json_get.return_value = {
            'using': {
                'model': 'dummy_model',
                'provider': 'dummy_provider',
                'column': 'non_existent_column'
            },
            'target': 'output_column'
        }

        # Create a DataFrame without the expected input column
        df = pd.DataFrame({'other_column': ['Test input']})

        # Assert that the correct exception is raised
        with self.assertRaisesRegex(RuntimeError, 'Column "non_existent_column" not found in input data'):
            self.handler.predict(df)

        # Verify that the mocked methods were called
        self.handler.model_storage.json_get.assert_called_once_with('args')
        mock_get_api_key.assert_called_once()
        mock_list_endpoints.assert_called_once()


if __name__ == '__main__':
    unittest.main()

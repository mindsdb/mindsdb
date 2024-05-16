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


if __name__ == '__main__':
    unittest.main()
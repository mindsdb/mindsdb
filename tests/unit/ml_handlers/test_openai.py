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


if __name__ == '__main__':
    unittest.main()
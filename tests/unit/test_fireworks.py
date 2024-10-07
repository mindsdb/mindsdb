import pandas as pd
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock
from pydantic import ValidationError

from mindsdb.integrations.handlers.fireworks_handler.fireworks_handler import (
    FireworksHandler,
)


class TestFireworks(unittest.TestCase):
    """
    Unit tests for the Fireworks handler.
    """

    dummy_connection_data = OrderedDict(
        fireworks_api_key="dummy_api_key",
    )

    def setUp(self):
        self.mock_model_storage = MagicMock()
        self.mock_engine_storage = MagicMock()
        self.handler = FireworksHandler(
            self.mock_model_storage,
            self.mock_engine_storage,
            connection_data={"connection_data": {}},
        )

    def test_create_validation_without_using_clause_raises_exception(self):
        """
        Test if model creation raises an exception without a USING clause.
        """

        with self.assertRaisesRegex(
            Exception,
            "fireworks_ai engine requires a USING clause! Refer to its documentation for more details.",
        ):
            self.handler.create_validation("target", args={}, handler_storage=None)

    def test_create_validation_with_invalid_args_raises_validation_error(self):
        """
        Test if `create_validation` raises Pydantic ValidationError for invalid arguments.
        """
        with self.assertRaises(ValidationError):
            self.handler.create_validation(
                "target", args={"using": {"invalid_field": "value"}}
            )

    def test_create_with_invalid_mode_raises_exception(self):
        """
        Test if `create` raises an exception when an unsupported mode is passed.
        """
        args = {"using": {"model": "some_model", "mode": "unsupported_mode"}}
        with self.assertRaises(Exception) as context:
            self.handler.create("target", args=args)
        self.assertTrue("Invalid operation mode" in str(context.exception))

    def test_predict_raises_runtime_error_for_invalid_column(self):
        """
        Test if `predict` raises RuntimeError when the input column is not in the dataframe.
        """
        # Mock model storage to return valid args for FireworksHandlerArgs
        self.mock_model_storage.json_get.return_value = {
            "column": "input_column",
            "target": "output_column",
            "mode": "conversational",
            "model": "some_model",
        }
        df = pd.DataFrame({"other_column": ["text1", "text2"]})
        args = {"using": {}}

        with self.assertRaises(RuntimeError) as context:
            self.handler.predict(df, args=args)
        self.assertTrue('Column "input_column" not found' in str(context.exception))


if __name__ == "__main__":
    unittest.main()

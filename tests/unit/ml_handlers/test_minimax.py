import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, patch

from mindsdb.integrations.handlers.minimax_handler.minimax_handler import MiniMaxHandler, MINIMAX_MODELS
from mindsdb.integrations.handlers.minimax_handler.settings import minimax_handler_config


class TestMiniMaxHandler(unittest.TestCase):
    """
    Unit tests for the MiniMax handler.
    """

    dummy_connection_data = OrderedDict(
        minimax_api_key="dummy_minimax_api_key",
    )

    def setUp(self):
        mock_engine_storage = MagicMock()
        mock_model_storage = MagicMock()
        mock_engine_storage.get_connection_args.return_value = self.dummy_connection_data
        mock_model_storage.json_get.return_value = {
            "using": {"prompt_template": "test prompt"},
            "target": "answer",
            "model_name": "MiniMax-M2.7",
            "api_base": minimax_handler_config.BASE_URL,
        }

        self.mock_engine_storage = mock_engine_storage
        self.handler = MiniMaxHandler(
            mock_model_storage,
            mock_engine_storage,
            connection_data={"connection_data": self.dummy_connection_data},
        )

    def test_default_base_url(self):
        """Test that the default base URL is the MiniMax overseas endpoint."""
        self.assertEqual(minimax_handler_config.BASE_URL, "https://api.minimax.io/v1")
        self.assertEqual(self.handler.api_base, "https://api.minimax.io/v1")

    def test_default_model(self):
        """Test that the default model is MiniMax-M2.7."""
        self.assertEqual(self.handler.default_model, "MiniMax-M2.7")

    def test_supported_models_list(self):
        """Test that MINIMAX_MODELS contains the expected models."""
        self.assertIn("MiniMax-M2.7", MINIMAX_MODELS)
        self.assertIn("MiniMax-M2.7-highspeed", MINIMAX_MODELS)

    def test_is_chat_model_returns_true_for_all_models(self):
        """Test that all MiniMax models are recognized as chat models."""
        for model in MINIMAX_MODELS:
            self.assertTrue(MiniMaxHandler.is_chat_model(model))

    def test_is_chat_model_returns_true_for_unknown_model(self):
        """Test that is_chat_model returns True for any model (MiniMax uses chat endpoint)."""
        self.assertTrue(MiniMaxHandler.is_chat_model("MiniMax-unknown"))

    def test_create_with_valid_model(self):
        """Test that create() succeeds with a valid model name."""
        args = {
            "using": {
                "prompt_template": "Answer the question: {{question}}",
                "model_name": "MiniMax-M2.7",
            }
        }
        # Should not raise
        self.handler.create(target="answer", args=args)

    def test_create_with_highspeed_model(self):
        """Test that create() succeeds with MiniMax-M2.7-highspeed."""
        args = {
            "using": {
                "prompt_template": "Answer the question: {{question}}",
                "model_name": "MiniMax-M2.7-highspeed",
            }
        }
        # Should not raise
        self.handler.create(target="answer", args=args)

    def test_create_with_invalid_model_raises_exception(self):
        """Test that create() raises an exception with an invalid model name."""
        args = {
            "using": {
                "prompt_template": "Answer the question: {{question}}",
                "model_name": "invalid-model",
            }
        }
        with self.assertRaises(Exception) as ctx:
            self.handler.create(target="answer", args=args)
        self.assertIn("Invalid model name", str(ctx.exception))
        self.assertIn("MiniMax-M2.7", str(ctx.exception))

    def test_create_without_model_uses_default(self):
        """Test that create() uses the default model when no model is specified."""
        args = {
            "using": {
                "prompt_template": "Answer the question: {{question}}",
            }
        }
        self.handler.create(target="answer", args=args)
        saved_args = self.handler.model_storage.json_set.call_args[0][1]
        self.assertEqual(saved_args["model_name"], "MiniMax-M2.7")

    def test_create_validation_without_using_clause_raises_exception(self):
        """Test that create_validation raises exception without USING clause."""
        with self.assertRaises(Exception) as ctx:
            MiniMaxHandler.create_validation(
                "target", args={}, handler_storage=self.mock_engine_storage
            )
        self.assertIn("USING clause", str(ctx.exception))

    @patch("mindsdb.integrations.handlers.minimax_handler.minimax_handler.OpenAIHandler._get_client")
    def test_create_validation_with_valid_api_key(self, mock_get_client):
        """Test that create_validation succeeds with a valid API key."""
        mock_client = MagicMock()
        mock_client.models.list.side_effect = Exception("404 page not found")
        mock_get_client.return_value = mock_client

        # Should not raise (404 is acceptable for MiniMax)
        MiniMaxHandler.create_validation(
            "target",
            args={"using": {"prompt_template": "test prompt"}},
            handler_storage=self.mock_engine_storage,
        )

    def test_check_client_connection_ignores_404(self):
        """Test that _check_client_connection ignores 404 errors."""
        mock_client = MagicMock()
        mock_client.models.list.side_effect = Exception("404 page not found")
        # Should not raise
        MiniMaxHandler._check_client_connection(mock_client)

    def test_check_client_connection_raises_on_auth_error(self):
        """Test that _check_client_connection raises on authentication error."""
        from openai import AuthenticationError
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"error": {"message": "Incorrect API key"}}
        mock_client.models.list.side_effect = AuthenticationError(
            "Incorrect API key", response=mock_response, body={"error": "auth_error"}
        )
        with self.assertRaises(Exception) as ctx:
            MiniMaxHandler._check_client_connection(mock_client)
        self.assertIn("Invalid MiniMax API key", str(ctx.exception))

    def test_finetune_raises_not_implemented(self):
        """Test that finetune raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.handler.finetune()


if __name__ == "__main__":
    unittest.main()

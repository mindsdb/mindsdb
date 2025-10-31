import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from mindsdb.integrations.handlers.lemonade_handler.lemonade_handler import LemonadeHandler


class TestLemonadeHandler(unittest.TestCase):
    """Test cases for LemonadeHandler"""

    def setUp(self):
        """Set up test fixtures"""
        self.engine_storage = Mock()
        self.model_storage = Mock()
        self.handler = LemonadeHandler(
            model_storage=self.model_storage,
            engine_storage=self.engine_storage
        )
        self.engine_storage.get_connection_args.return_value = {}
        self.model_storage.json_get.return_value = {
            "model_name": "Llama-3.2-1B-Instruct-Hybrid",
            "mode": "default",
            "target": "answer",
            "question_column": "question"
        }

    def test_is_chat_model(self):
        """Test chat model detection"""
        # Test chat models
        self.assertTrue(LemonadeHandler.is_chat_model("llama-3.2-1b-instruct"))
        self.assertTrue(LemonadeHandler.is_chat_model("mistral-7b-instruct"))
        self.assertTrue(LemonadeHandler.is_chat_model("gemma-3-4b-it"))
        
        # Test non-chat models
        self.assertFalse(LemonadeHandler.is_chat_model("random-model"))

    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.OpenAI')
    def test_get_client(self, mock_openai):
        """Test client creation"""
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        client = LemonadeHandler._get_client(
            api_key="test-key",
            base_url="http://localhost:8000/api/v1",
            org=None,
            args={}
        )
        
        mock_openai.assert_called_once_with(
            api_key="test-key",
            base_url="http://localhost:8000/api/v1",
            organization=None
        )
        self.assertEqual(client, mock_client)

    def test_supported_modes(self):
        """Test supported modes"""
        expected_modes = ["default", "conversational", "conversational-full"]
        self.assertEqual(self.handler.supported_modes, expected_modes)

    def test_default_values(self):
        """Test default values"""
        self.assertEqual(self.handler.name, "lemonade")
        self.assertEqual(self.handler.default_model, "Llama-3.2-1B-Instruct-Hybrid")
        self.assertEqual(self.handler.default_mode, "default")
        self.assertTrue(self.handler.generative)

    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.get_api_key')
    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.OpenAI')
    def test_create_engine(self, mock_openai, mock_get_api_key):
        """Test engine creation"""
        mock_get_api_key.return_value = "test-key"
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        connection_args = {
            "lemonade_api_key": "test-key",
            "api_base": "http://localhost:8000/api/v1"
        }
        
        # Should not raise an exception
        self.handler.create_engine(connection_args)
        
        mock_openai.assert_called_once_with(
            api_key="test-key",
            base_url="http://localhost:8000/api/v1",
            organization=None
        )

    def test_finetune_not_supported(self):
        """Test that fine-tuning is not supported"""
        with self.assertRaises(Exception) as context:
            self.handler.finetune()
        
        self.assertIn("Fine-tuning is not supported by Lemonade", str(context.exception))

    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.get_api_key')
    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.OpenAI')
    def test_describe_args(self, mock_openai, mock_get_api_key):
        """Test describe method with args attribute"""
        mock_get_api_key.return_value = "test-key"
        mock_client = Mock()
        mock_openai.return_value = mock_client
        
        result = self.handler.describe("args")
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["key", "value"])

    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.get_api_key')
    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.OpenAI')
    def test_describe_metadata(self, mock_openai, mock_get_api_key):
        """Test describe method with metadata attribute"""
        mock_get_api_key.return_value = "test-key"
        mock_client = Mock()
        mock_openai.return_value = mock_client
        mock_client.models.retrieve.return_value = {"id": "test-model", "object": "model"}
        
        result = self.handler.describe("metadata")
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["key", "value"])

    @patch('mindsdb.integrations.handlers.lemonade_handler.lemonade_handler.get_api_key')
    def test_describe_tables(self, mock_get_api_key):
        """Test describe method with no attribute"""
        mock_get_api_key.return_value = "test-key"
        result = self.handler.describe()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["tables"])
        self.assertEqual(list(result["tables"]), ["args", "metadata"])


if __name__ == '__main__':
    unittest.main()

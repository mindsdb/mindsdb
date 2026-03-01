import unittest
from unittest.mock import patch, Mock
import pandas as pd
from mindsdb.integrations.handlers.ollama_handler.ollama_handler import OllamaHandler

class TestOllamaHandler(unittest.TestCase):
    
    def setUp(self):
        # Mock the storage to return valid model configuration
        mock_storage = Mock()
        mock_storage.json_get.return_value = {
            'model_name': 'tinyllama',
            'target': 'response',
            'ollama_serve_url': 'http://localhost:11434'
        }

        # Initialize handler with mocked storage
        self.handler = OllamaHandler(name='test_ollama', model_storage=mock_storage, engine_storage={})

    @patch('mindsdb.integrations.handlers.ollama_handler.ollama_handler.requests.post')
    def test_temperature_passing(self, mock_post):
        """
        Test that the temperature parameter is correctly extracted from args
        and passed to the Ollama API options.
        """
        # Setup mock response
        mock_response = Mock()
        mock_response.content = b'{"response": "Test response"}'
        mock_post.return_value = mock_response

        # Create input dataframe
        df = pd.DataFrame({'text': ['Hello']})
        
        # Execute prediction with temperature argument
        self.handler.predict(df, args={'predict_params': {'temperature': 0.5}})

        # Verify API call payload
        call_args = mock_post.call_args[1]['json']
        
        self.assertIn('options', call_args)
        self.assertEqual(call_args['options']['temperature'], 0.5)

if __name__ == '__main__':
    unittest.main()
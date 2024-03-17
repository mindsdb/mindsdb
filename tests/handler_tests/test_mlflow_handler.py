import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests
from mindsdb.integrations.handlers.mlflow_handler.mlflow_handler import MLflowHandler

class TestMLflowHandler(unittest.TestCase):
    def setUp(self):
        self.mock_model_storage = Mock()
        self.mock_model_storage.json_get.return_value = {
            'mlflow_server_url': 'http://localhost:5000',
            'mlflow_server_path': '/mlflow',
            'model_name': 'test_model',
            'predict_url': 'http://localhost:8080/invocations',
            'target': 'target_column'
        }
        self.mock_engine_storage = Mock()

        self.handler = MLflowHandler(self.mock_model_storage, self.mock_engine_storage)
        self.args = {
            'using': {
                'mlflow_server_url': 'http://localhost:5000',
                'mlflow_server_path': '/mlflow',
                'model_name': 'test_model',
                'predict_url': 'http://localhost:8080/invocations'
            }
        }
        self.target = 'target_column'
        self.data = pd.DataFrame({'col1': [1, 2, 3], 'col2': [4, 5, 6]})

    
    @patch('mlflow.tracking.MlflowClient')
    @patch('requests.post')
    def test_create_with_invalid_model(self, mock_post, mock_mlflow_client):
        mock_post.return_value.status_code = 200
        mock_connection = Mock()
        mock_mlflow_client.return_value = mock_connection
        mock_connection.search_registered_models.return_value = [Mock(name='other_model')]

        with self.assertRaises(Exception):
            self.handler.create(self.target, args=self.args)

    @patch('requests.post')
    def test_predict(self, mock_post):
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = [0.1, 0.2, 0.3]

        result = self.handler.predict(self.data)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(list(result[self.target]), [0.1, 0.2, 0.3])

    def test_describe_tables(self):
        result = self.handler.describe('invalid_key')
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(list(result['tables']), ['info'])

    @patch('requests.post')
    def test_check_model_url_valid(self, mock_post):
        mock_post.return_value.status_code = 200
        with patch('requests.post') as mock_post_inner:
            mock_post_inner.return_value.status_code = 200
            self.handler._check_model_url('http://localhost:8080/invocations')

    @patch('requests.post')
    def test_check_model_url_invalid_status_code(self, mock_post):
        mock_post.return_value.status_code = 404
        with patch('requests.post') as mock_post_inner:
            mock_post_inner.return_value.status_code = 404
            with self.assertRaises(Exception):
                self.handler._check_model_url('http://localhost:8080/invocations')

    @patch('requests.post')
    def test_check_model_url_request_exception(self, mock_post):
        mock_post.side_effect = requests.RequestException('Connection error')
        with self.assertRaises(Exception):
            self.handler._check_model_url('http://localhost:8080/invocations')

if __name__ == '__main__':
    unittest.main()

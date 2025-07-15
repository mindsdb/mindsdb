import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from datetime import datetime

from mindsdb.integrations.handlers.raindrop_handler.raindrop_handler import RaindropHandler, RaindropAPIClient
from mindsdb.integrations.handlers.raindrop_handler.raindrop_tables import RaindropsTable, CollectionsTable


class TestRaindropHandler(unittest.TestCase):
    """Test cases for RaindropHandler"""

    def setUp(self):
        self.handler = RaindropHandler('test_raindrop_handler')
        self.handler.connection_data = {'api_key': 'test_api_key'}
        
    def test_init(self):
        """Test handler initialization"""
        self.assertEqual(self.handler.name, 'test_raindrop_handler')
        self.assertFalse(self.handler.is_connected)
        self.assertIn('raindrops', self.handler._tables)
        self.assertIn('bookmarks', self.handler._tables)
        self.assertIn('collections', self.handler._tables)

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient')
    def test_connect(self, mock_client):
        """Test connection establishment"""
        mock_instance = Mock()
        mock_client.return_value = mock_instance
        
        result = self.handler.connect()
        
        mock_client.assert_called_once_with('test_api_key')
        self.assertEqual(result, mock_instance)
        self.assertTrue(self.handler.is_connected)

    def test_connect_missing_api_key(self):
        """Test connection with missing API key"""
        self.handler.connection_data = {}
        
        with self.assertRaises(ValueError) as context:
            self.handler.connect()
        
        self.assertIn("API key is required", str(context.exception))

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient')
    def test_check_connection_success(self, mock_client):
        """Test successful connection check"""
        mock_instance = Mock()
        mock_instance.get_user_stats.return_value = {'result': True}
        mock_client.return_value = mock_instance
        
        result = self.handler.check_connection()
        
        self.assertTrue(result.success)
        self.assertTrue(self.handler.is_connected)

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient')
    def test_check_connection_failure(self, mock_client):
        """Test failed connection check"""
        mock_instance = Mock()
        mock_instance.get_user_stats.return_value = {'result': False}
        mock_client.return_value = mock_instance
        
        result = self.handler.check_connection()
        
        self.assertFalse(result.success)
        self.assertFalse(self.handler.is_connected)

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.RaindropAPIClient')
    def test_check_connection_exception(self, mock_client):
        """Test connection check with exception"""
        mock_instance = Mock()
        mock_instance.get_user_stats.side_effect = Exception("Connection error")
        mock_client.return_value = mock_instance
        
        result = self.handler.check_connection()
        
        self.assertFalse(result.success)
        self.assertIn("Connection error", result.error_message)


class TestRaindropAPIClient(unittest.TestCase):
    """Test cases for RaindropAPIClient"""

    def setUp(self):
        self.client = RaindropAPIClient('test_api_key')

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.requests')
    def test_make_request_get(self, mock_requests):
        """Test GET request"""
        mock_response = Mock()
        mock_response.json.return_value = {'result': True, 'items': []}
        mock_response.raise_for_status.return_value = None
        mock_requests.request.return_value = mock_response
        
        result = self.client._make_request('GET', '/user/stats')
        
        mock_requests.request.assert_called_once_with(
            method='GET',
            url='https://api.raindrop.io/rest/v1/user/stats',
            headers={
                'Authorization': 'Bearer test_api_key',
                'Content-Type': 'application/json'
            },
            params=None,
            json=None
        )
        self.assertEqual(result, {'result': True, 'items': []})

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_handler.requests')
    def test_make_request_post(self, mock_requests):
        """Test POST request with data"""
        mock_response = Mock()
        mock_response.json.return_value = {'result': True, 'item': {}}
        mock_response.raise_for_status.return_value = None
        mock_requests.request.return_value = mock_response
        
        test_data = {'title': 'Test'}
        result = self.client._make_request('POST', '/raindrop', data=test_data)
        
        mock_requests.request.assert_called_once_with(
            method='POST',
            url='https://api.raindrop.io/rest/v1/raindrop',
            headers={
                'Authorization': 'Bearer test_api_key',
                'Content-Type': 'application/json'
            },
            params=None,
            json=test_data
        )

    @patch.object(RaindropAPIClient, '_make_request')
    def test_get_raindrops(self, mock_request):
        """Test get_raindrops method"""
        mock_request.return_value = {'result': True, 'items': []}
        
        result = self.client.get_raindrops(collection_id=123, search='test', page=1)
        
        mock_request.assert_called_once_with(
            'GET', 
            '/raindrops/123',
            params={'page': 1, 'perpage': 50, 'search': 'test'}
        )

    @patch.object(RaindropAPIClient, '_make_request')
    def test_create_raindrop(self, mock_request):
        """Test create_raindrop method"""
        mock_request.return_value = {'result': True, 'item': {}}
        
        raindrop_data = {'link': 'https://example.com', 'title': 'Test'}
        result = self.client.create_raindrop(raindrop_data)
        
        mock_request.assert_called_once_with('POST', '/raindrop', data=raindrop_data)

    @patch.object(RaindropAPIClient, '_make_request')
    def test_update_raindrop(self, mock_request):
        """Test update_raindrop method"""
        mock_request.return_value = {'result': True, 'item': {}}
        
        raindrop_data = {'title': 'Updated Title'}
        result = self.client.update_raindrop(123, raindrop_data)
        
        mock_request.assert_called_once_with('PUT', '/raindrop/123', data=raindrop_data)

    @patch.object(RaindropAPIClient, '_make_request')
    def test_delete_raindrop(self, mock_request):
        """Test delete_raindrop method"""
        mock_request.return_value = {'result': True}
        
        result = self.client.delete_raindrop(123)
        
        mock_request.assert_called_once_with('DELETE', '/raindrop/123')

    def test_make_request_invalid_endpoint(self):
        """Test that invalid endpoints are rejected"""
        with self.assertRaises(ValueError) as context:
            self.client._make_request('GET', '/invalid/endpoint')
        
        self.assertIn("Invalid endpoint", str(context.exception))
        self.assertIn("Only Raindrop.io API endpoints are allowed", str(context.exception))

    def test_make_request_path_traversal_attempt(self):
        """Test that path traversal attempts are rejected"""
        with self.assertRaises(ValueError) as context:
            self.client._make_request('GET', '../../../etc/passwd')
        
        self.assertIn("Invalid endpoint", str(context.exception))


class TestRaindropsTable(unittest.TestCase):
    """Test cases for RaindropsTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = RaindropsTable(self.handler)

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()
        
        expected_columns = [
            "_id", "link", "title", "excerpt", "note", "type", "cover", "tags",
            "important", "reminder", "removed", "created", "lastUpdate",
            "domain", "collection.id", "collection.title", "user.id", "broken",
            "cache", "file.name", "file.size", "file.type"
        ]
        
        self.assertEqual(columns, expected_columns)

    def test_normalize_raindrop_data(self):
        """Test _normalize_raindrop_data method"""
        test_data = pd.DataFrame([{
            '_id': 123,
            'title': 'Test',
            'collection': {'$id': 456, 'title': 'Test Collection'},
            'user': {'$id': 789},
            'file': {'name': 'test.pdf', 'size': 1024, 'type': 'pdf'},
            'tags': ['tag1', 'tag2'],
            'created': '2024-01-01T00:00:00Z',
            'lastUpdate': '2024-01-02T00:00:00Z'
        }])
        
        result = self.table._normalize_raindrop_data(test_data)
        
        self.assertEqual(result['collection.id'].iloc[0], 456)
        self.assertEqual(result['collection.title'].iloc[0], 'Test Collection')
        self.assertEqual(result['user.id'].iloc[0], 789)
        self.assertEqual(result['file.name'].iloc[0], 'test.pdf')
        self.assertEqual(result['file.size'].iloc[0], 1024)
        self.assertEqual(result['file.type'].iloc[0], 'pdf')
        self.assertEqual(result['tags'].iloc[0], 'tag1,tag2')

    def test_prepare_raindrop_data(self):
        """Test _prepare_raindrop_data method"""
        input_data = {
            'link': 'https://example.com',
            'title': 'Test',
            'collection_id': 123,
            'tags': 'tag1,tag2',
            'important': True
        }
        
        result = self.table._prepare_raindrop_data(input_data)
        
        expected = {
            'link': 'https://example.com',
            'title': 'Test',
            'collection': {'$id': 123},
            'tags': ['tag1', 'tag2'],
            'important': True
        }
        
        self.assertEqual(result, expected)

    def test_prepare_raindrop_data_with_list_tags(self):
        """Test _prepare_raindrop_data method with list tags"""
        input_data = {
            'link': 'https://example.com',
            'tags': ['tag1', 'tag2']
        }
        
        result = self.table._prepare_raindrop_data(input_data)
        
        self.assertEqual(result['tags'], ['tag1', 'tag2'])

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryParser')
    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.SELECTQueryExecutor')
    def test_select_basic(self, mock_executor, mock_parser):
        """Test basic select operation"""
        # Mock parser
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = ([], [], [], None)
        mock_parser.return_value = mock_parser_instance
        
        # Mock executor
        mock_executor_instance = Mock()
        mock_executor_instance.execute_query.return_value = pd.DataFrame()
        mock_executor.return_value = mock_executor_instance
        
        # Mock API response
        self.handler.connection.get_raindrops.return_value = {
            'result': True,
            'items': [{'_id': 123, 'title': 'Test'}]
        }
        
        query = Mock()
        result = self.table.select(query)
        
        self.assertIsInstance(result, pd.DataFrame)

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.INSERTQueryParser')
    def test_insert_single(self, mock_parser):
        """Test single insert operation"""
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = {
            'link': 'https://example.com',
            'title': 'Test'
        }
        mock_parser.return_value = mock_parser_instance
        
        self.handler.connection.create_raindrop.return_value = {'result': True}
        
        query = Mock()
        self.table.insert(query)
        
        self.handler.connection.create_raindrop.assert_called_once()

    @patch('mindsdb.integrations.handlers.raindrop_handler.raindrop_tables.INSERTQueryParser')
    def test_insert_multiple(self, mock_parser):
        """Test multiple insert operation"""
        mock_parser_instance = Mock()
        mock_parser_instance.parse_query.return_value = [
            {'link': 'https://example1.com', 'title': 'Test1'},
            {'link': 'https://example2.com', 'title': 'Test2'}
        ]
        mock_parser.return_value = mock_parser_instance
        
        self.handler.connection.create_multiple_raindrops.return_value = {'result': True}
        
        query = Mock()
        self.table.insert(query)
        
        self.handler.connection.create_multiple_raindrops.assert_called_once()


class TestCollectionsTable(unittest.TestCase):
    """Test cases for CollectionsTable"""

    def setUp(self):
        self.handler = Mock()
        self.handler.connection = Mock()
        self.table = CollectionsTable(self.handler)

    def test_get_columns(self):
        """Test get_columns method"""
        columns = self.table.get_columns()
        
        expected_columns = [
            "_id", "title", "description", "color", "view", "public", "sort",
            "count", "created", "lastUpdate", "expanded", "parent.id", "user.id",
            "cover", "access.level", "access.draggable"
        ]
        
        self.assertEqual(columns, expected_columns)

    def test_normalize_collection_data(self):
        """Test _normalize_collection_data method"""
        test_data = pd.DataFrame([{
            '_id': 123,
            'title': 'Test Collection',
            'parent': {'$id': 456},
            'user': {'$id': 789},
            'access': {'level': 4, 'draggable': True},
            'cover': ['https://example.com/cover.jpg'],
            'created': '2024-01-01T00:00:00Z',
            'lastUpdate': '2024-01-02T00:00:00Z'
        }])
        
        result = self.table._normalize_collection_data(test_data)
        
        self.assertEqual(result['parent.id'].iloc[0], 456)
        self.assertEqual(result['user.id'].iloc[0], 789)
        self.assertEqual(result['access.level'].iloc[0], 4)
        self.assertEqual(result['access.draggable'].iloc[0], True)
        self.assertEqual(result['cover'].iloc[0], 'https://example.com/cover.jpg')

    def test_prepare_collection_data(self):
        """Test _prepare_collection_data method"""
        input_data = {
            'title': 'Test Collection',
            'description': 'Test Description',
            'color': '#FF0000',
            'public': True,
            'parent_id': 123
        }
        
        result = self.table._prepare_collection_data(input_data)
        
        expected = {
            'title': 'Test Collection',
            'description': 'Test Description',
            'color': '#FF0000',
            'public': True,
            'parent': {'$id': 123}
        }
        
        self.assertEqual(result, expected)

    def test_get_collections(self):
        """Test get_collections method"""
        self.handler.connection.get_collections.return_value = {
            'result': True,
            'items': [{'_id': 123, 'title': 'Root Collection'}]
        }
        self.handler.connection.get_child_collections.return_value = {
            'result': True,
            'items': [{'_id': 456, 'title': 'Child Collection'}]
        }
        
        result = self.table.get_collections()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['_id'], 123)
        self.assertEqual(result[1]['_id'], 456)


if __name__ == '__main__':
    unittest.main()

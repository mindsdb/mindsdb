import unittest
from unittest.mock import Mock, patch
import pandas as pd
import requests
import sys
import os

# Add parent directory to path so we can import our handler
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import our handler modules
from coinmarketcap_handler import CoinMarketCapHandler
from coinmarketcap_tables import ListingTable, QuotesTable, InfoTable, GlobalMetricsTable
from connection_args import connection_args, connection_args_example

# MindsDB imports
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse, RESPONSE_TYPE
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE


class TestCoinMarketCapHandler(unittest.TestCase):
    """Test suite for CoinMarketCapHandler"""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.connection_data = {
            'api_key': 'test_api_key_12345',
            'base_url': 'https://pro-api.coinmarketcap.com'
        }
        
        self.handler = CoinMarketCapHandler(
            name='test_coinmarketcap',
            connection_data=self.connection_data
        )
        
        # Sample API response for mocking
        self.sample_listings_response = {
            'status': {'error_code': 0, 'error_message': None},
            'data': [
                {
                    'id': 1,
                    'name': 'Bitcoin',
                    'symbol': 'BTC',
                    'slug': 'bitcoin',
                    'cmc_rank': 1,
                    'circulating_supply': 19500000,
                    'total_supply': 19500000,
                    'max_supply': 21000000,
                    'quote': {
                        'USD': {
                            'price': 45000.50,
                            'volume_24h': 25000000000,
                            'market_cap': 877500000000,
                            'percent_change_1h': -0.5,
                            'percent_change_24h': 2.3,
                            'percent_change_7d': -1.2,
                            'last_updated': '2025-06-03T10:00:00.000Z'
                        }
                    }
                }
            ]
        }

    def test_handler_initialization(self):
        """Test handler initialization with connection data"""
        self.assertEqual(self.handler.api_key, 'test_api_key_12345')
        self.assertEqual(self.handler.base_url, 'https://pro-api.coinmarketcap.com')
        self.assertFalse(self.handler.is_connected)
        
        # Check that tables are registered
        expected_tables = ['listings', 'quotes', 'info', 'global_metrics']
        for table_name in expected_tables:
            self.assertTrue(self.handler._table_exists(table_name))

    @patch('requests.get')
    def test_connect_success(self, mock_get):
        """Test successful connection to CoinMarketCap API"""
        # Mock successful API response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'status': {'error_code': 0}}
        mock_get.return_value = mock_response
        
        # Test connection
        result = self.handler.connect()
        
        self.assertTrue(result.success)
        self.assertTrue(self.handler.is_connected)
        
        # Verify API call was made correctly
        mock_get.assert_called_once()
        call_args = mock_get.call_args
        self.assertIn('X-CMC_PRO_API_KEY', call_args[1]['headers'])

    @patch('requests.get')
    def test_connect_failure_invalid_key(self, mock_get):
        """Test connection failure with invalid API key"""
        # Mock 401 response
        mock_response = Mock()
        mock_response.status_code = 401
        mock_get.return_value = mock_response
        
        result = self.handler.connect()
        
        self.assertFalse(result.success)
        self.assertIn('401', result.error_message)

    @patch('requests.get')
    def test_connect_network_error(self, mock_get):
        """Test connection failure due to network error"""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network error")
        
        result = self.handler.connect()
        
        self.assertFalse(result.success)
        self.assertIn('Connection error', result.error_message)

    def test_get_tables(self):
        """Test get_tables method returns correct table list"""
        result = self.handler.get_tables()
        
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        
        # Check that all expected tables are present
        table_names = result.data_frame['TABLE_NAME'].tolist()
        expected_tables = ['listings', 'quotes', 'info', 'global_metrics']
        
        for table in expected_tables:
            self.assertIn(table, table_names)

    def test_get_columns_listings_table(self):
        """Test get_columns for listings table"""
        result = self.handler.get_columns('listings')
        
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        
        # Check expected columns are present
        column_names = result.data_frame['COLUMN_NAME'].tolist()
        expected_columns = ['id', 'name', 'symbol', 'price', 'market_cap']
        
        for col in expected_columns:
            self.assertIn(col, column_names)

    def test_get_columns_nonexistent_table(self):
        """Test get_columns for non-existent table"""
        result = self.handler.get_columns('nonexistent_table')
        
        self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
        self.assertIn('not found', result.error_message.lower())

    @patch('requests.get')
    def test_call_api_success(self, mock_get):
        """Test successful API call"""
        # Set handler as connected
        self.handler.is_connected = True
        
        # Mock successful response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = self.sample_listings_response
        mock_get.return_value = mock_response
        
        # Test API call
        result = self.handler.call_coinmarketcap_api('/v1/cryptocurrency/listings/latest')
        
        self.assertEqual(result, self.sample_listings_response)

    def test_call_api_not_connected(self):
        """Test API call when not connected"""
        with self.assertRaises(Exception) as context:
            self.handler.call_coinmarketcap_api('/v1/test')
        
        self.assertIn('Not connected', str(context.exception))

    @patch('requests.get')
    def test_call_api_rate_limit(self, mock_get):
        """Test API call with rate limit error"""
        self.handler.is_connected = True
        
        mock_response = Mock()
        mock_response.status_code = 429
        mock_get.return_value = mock_response
        
        with self.assertRaises(Exception) as context:
            self.handler.call_coinmarketcap_api('/v1/test')
        
        self.assertIn('Rate limit exceeded', str(context.exception))

    def test_table_exists(self):
        """Test table existence checking"""
        self.assertTrue(self.handler._table_exists('listings'))
        self.assertTrue(self.handler._table_exists('quotes'))
        self.assertFalse(self.handler._table_exists('nonexistent'))

    def test_get_table_instance(self):
        """Test getting table instances"""
        listings_table = self.handler._get_table('listings')
        self.assertIsInstance(listings_table, ListingTable)
        
        quotes_table = self.handler._get_table('quotes')
        self.assertIsInstance(quotes_table, QuotesTable)
        
        invalid_table = self.handler._get_table('nonexistent')
        self.assertIsNone(invalid_table)


class TestListingTable(unittest.TestCase):
    """Test suite for ListingTable"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_handler = Mock()
        self.mock_handler.call_coinmarketcap_api.return_value = {
            'data': [
                {
                    'id': 1,
                    'name': 'Bitcoin',
                    'symbol': 'BTC',
                    'slug': 'bitcoin',
                    'cmc_rank': 1,
                    'circulating_supply': 19500000,
                    'total_supply': 19500000,
                    'max_supply': 21000000,
                    'quote': {
                        'USD': {
                            'price': 45000.50,
                            'volume_24h': 25000000000,
                            'market_cap': 877500000000,
                            'percent_change_1h': -0.5,
                            'percent_change_24h': 2.3,
                            'percent_change_7d': -1.2,
                            'last_updated': '2025-06-03T10:00:00.000Z'
                        }
                    }
                }
            ]
        }
        
        self.table = ListingTable(self.mock_handler)

    def test_get_columns(self):
        """Test get_columns method returns correct column definitions"""
        result = self.table.get_columns()
        
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertIsInstance(result.data_frame, pd.DataFrame)
        
        # Check that expected columns are present
        column_names = result.data_frame['name'].tolist()
        expected_columns = ['id', 'name', 'symbol', 'price', 'market_cap']
        
        for col in expected_columns:
            self.assertIn(col, column_names)

    def test_parse_listings_data(self):
        """Test parsing of listings API response"""
        api_response = {
            'data': [
                {
                    'id': 1,
                    'name': 'Bitcoin',
                    'symbol': 'BTC',
                    'slug': 'bitcoin',
                    'cmc_rank': 1,
                    'circulating_supply': 19500000,
                    'total_supply': 19500000,
                    'max_supply': 21000000,
                    'quote': {
                        'USD': {
                            'price': 45000.50,
                            'volume_24h': 25000000000,
                            'market_cap': 877500000000,
                            'percent_change_1h': -0.5,
                            'percent_change_24h': 2.3,
                            'percent_change_7d': -1.2,
                            'last_updated': '2025-06-03T10:00:00.000Z'
                        }
                    }
                }
            ]
        }
        
        df = self.table._parse_listings_data(api_response)
        
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Bitcoin')
        self.assertEqual(df.iloc[0]['symbol'], 'BTC')
        self.assertEqual(df.iloc[0]['price'], 45000.50)


class TestConnectionArgs(unittest.TestCase):
    """Test suite for connection arguments"""
    
    def test_connection_args_structure(self):
        """Test connection arguments have correct structure"""
        self.assertIn('api_key', connection_args)
        self.assertIn('base_url', connection_args)
        
        # Test api_key configuration
        api_key_config = connection_args['api_key']
        self.assertEqual(api_key_config['type'], ARG_TYPE.PWD)
        self.assertTrue(api_key_config['required'])
        self.assertTrue(api_key_config['secret'])
        
        # Test base_url configuration
        base_url_config = connection_args['base_url']
        self.assertEqual(base_url_config['type'], ARG_TYPE.STR)
        self.assertFalse(base_url_config['required'])

    def test_connection_args_example(self):
        """Test connection arguments example is valid"""
        self.assertIn('api_key', connection_args_example)
        self.assertIn('base_url', connection_args_example)
        self.assertIn('coinmarketcap.com', connection_args_example['base_url'])


if __name__ == '__main__':
    # Run all tests
    unittest.main(verbosity=2)
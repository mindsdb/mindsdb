
import unittest
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add the parent directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

import pandas as pd
import requests

# Import MindsDB components
from mindsdb.integrations.libs.response import HandlerResponse, HandlerStatusResponse, RESPONSE_TYPE
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE

# Import our handler components
from coinmarketcap_handler import CoinMarketCapHandler
from coinmarketcap_tables import ListingTable, QuotesTable, InfoTable, GlobalMetricsTable
from connection_args import connection_args, connection_args_example


class TestCoinMarketCapHandlerEnhanced(unittest.TestCase):
    """Enhanced test suite for CoinMarketCapHandler with better coverage"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.connection_data = {
            'api_key': 'test_api_key_12345',
            'base_url': 'https://pro-api.coinmarketcap.com'
        }
        
        self.handler = CoinMarketCapHandler(
            name='test_coinmarketcap',
            connection_data=self.connection_data
        )

    def test_handler_initialization_defaults(self):
        """Test handler initialization with defaults"""
        # Test with minimal connection data
        minimal_data = {'api_key': 'test_key'}
        handler = CoinMarketCapHandler('test', connection_data=minimal_data)
        
        self.assertEqual(handler.api_key, 'test_key')
        self.assertEqual(handler.base_url, 'https://pro-api.coinmarketcap.com')  # Default
        self.assertFalse(handler.is_connected)

    def test_handler_initialization_empty_data(self):
        """Test handler initialization with empty connection data"""
        handler = CoinMarketCapHandler('test', connection_data={})
        
        self.assertIsNone(handler.api_key)
        self.assertEqual(handler.base_url, 'https://pro-api.coinmarketcap.com')

    def test_check_connection_delegates_to_connect(self):
        """Test that check_connection calls connect"""
        with patch.object(self.handler, 'connect') as mock_connect:
            mock_connect.return_value = HandlerStatusResponse(True)
            
            result = self.handler.check_connection()
            
            mock_connect.assert_called_once()
            self.assertTrue(result.success)

    def test_connect_already_connected(self):
        """Test connect when already connected"""
        self.handler.is_connected = True
        
        result = self.handler.connect()
        
        self.assertTrue(result.success)

    @patch('requests.get')
    def test_connect_various_error_codes(self, mock_get):
        """Test connect with various HTTP error codes"""
        error_codes = [401, 403, 429, 500]
        
        for code in error_codes:
            with self.subTest(error_code=code):
                mock_response = Mock()
                mock_response.status_code = code
                mock_get.return_value = mock_response
                
                result = self.handler.connect()
                
                self.assertFalse(result.success)
                self.assertIn(str(code), result.error_message)

    @patch('requests.get')
    def test_connect_timeout_error(self, mock_get):
        """Test connect with timeout error"""
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
        
        result = self.handler.connect()
        
        self.assertFalse(result.success)
        self.assertIn('Connection error', result.error_message)

    @patch('requests.get')
    def test_connect_request_exception(self, mock_get):
        """Test connect with general request exception"""
        mock_get.side_effect = requests.exceptions.RequestException("General error")
        
        result = self.handler.connect()
        
        self.assertFalse(result.success)
        self.assertIn('Connection error', result.error_message)

    def test_query_not_connected_auto_connect_fails(self):
        """Test query when not connected and auto-connect fails"""
        mock_query = Mock()
        mock_query.from_table = 'listings'
        
        with patch.object(self.handler, 'connect') as mock_connect:
            mock_connect.return_value = HandlerStatusResponse(False, error_message="Connection failed")
            
            result = self.handler.query(mock_query)
            
            self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
            self.assertIn('Failed to connect', result.error_message)

    def test_query_table_not_found(self):
        """Test query with non-existent table"""
        self.handler.is_connected = True
        
        mock_query = Mock()
        mock_query.from_table = 'nonexistent_table'
        
        result = self.handler.query(mock_query)
        
        self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
        self.assertIn('not found', result.error_message)

    def test_query_table_instance_failure(self):
        """Test query when table instance cannot be retrieved"""
        self.handler.is_connected = True
        
        # Mock a table that exists but returns None instance
        with patch.object(self.handler, '_table_exists', return_value=True):
            with patch.object(self.handler, '_get_table', return_value=None):
                mock_query = Mock()
                mock_query.from_table = 'listings'
                
                result = self.handler.query(mock_query)
                
                self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
                self.assertIn('Failed to get table instance', result.error_message)

    def test_query_table_execution_exception(self):
        """Test query when table execution raises exception"""
        self.handler.is_connected = True
        
        mock_table = Mock()
        mock_table.select.side_effect = Exception("Table error")
        
        with patch.object(self.handler, '_get_table', return_value=mock_table):
            mock_query = Mock()
            mock_query.from_table = 'listings'
            
            result = self.handler.query(mock_query)
            
            self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
            self.assertIn('Query execution failed', result.error_message)

    def test_native_query_not_implemented(self):
        """Test that native_query raises NotImplementedError"""
        with self.assertRaises(NotImplementedError):
            self.handler.native_query("SELECT * FROM test")

    def test_normalize_table_name_with_identifier(self):
        """Test _normalize_table_name with Identifier-like object"""
        # Mock object with parts attribute
        mock_identifier = Mock()
        mock_identifier.parts = ['database', 'schema', 'table_name']
        
        result = self.handler._normalize_table_name(mock_identifier)
        self.assertEqual(result, 'table_name')

    def test_normalize_table_name_with_string(self):
        """Test _normalize_table_name with string"""
        result = self.handler._normalize_table_name('simple_table')
        self.assertEqual(result, 'simple_table')

    def test_get_table_names(self):
        """Test _get_table_names method"""
        table_names = self.handler._get_table_names()
        expected_tables = ['listings', 'quotes', 'info', 'global_metrics']
        
        self.assertEqual(set(table_names), set(expected_tables))

    def test_get_table_description(self):
        """Test _get_table_description method"""
        descriptions = {
            'listings': self.handler._get_table_description('listings'),
            'quotes': self.handler._get_table_description('quotes'),
            'info': self.handler._get_table_description('info'),
            'global_metrics': self.handler._get_table_description('global_metrics'),
            'unknown': self.handler._get_table_description('unknown_table')
        }
        
        # Check that all return strings
        for table, desc in descriptions.items():
            self.assertIsInstance(desc, str)
            self.assertGreater(len(desc), 0)

    def test_get_columns_error_response_from_table(self):
        """Test get_columns when table returns error response"""
        mock_table = Mock()
        mock_table.get_columns.return_value = HandlerResponse(
            RESPONSE_TYPE.ERROR, 
            error_message="Table error"
        )
        
        with patch.object(self.handler, '_get_table', return_value=mock_table):
            result = self.handler.get_columns('listings')
            
            self.assertEqual(result.type, RESPONSE_TYPE.ERROR)

    def test_get_columns_missing_expected_columns(self):
        """Test get_columns when table response missing expected columns"""
        mock_table = Mock()
        
        # Create DataFrame missing required columns
        incomplete_df = pd.DataFrame({'name': ['col1'], 'missing_type': ['int']})
        mock_table.get_columns.return_value = HandlerResponse(
            RESPONSE_TYPE.TABLE,
            incomplete_df
        )
        
        with patch.object(self.handler, '_get_table', return_value=mock_table):
            result = self.handler.get_columns('listings')
            
            self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
            self.assertIn('missing columns', result.error_message)

    def test_get_columns_exception_handling(self):
        """Test get_columns exception handling"""
        with patch.object(self.handler, '_get_table', side_effect=Exception("Unexpected error")):
            result = self.handler.get_columns('listings')
            
            self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
            self.assertIn('Error getting columns', result.error_message)

    @patch('requests.get')
    def test_call_api_various_error_scenarios(self, mock_get):
        """Test API call with various error scenarios"""
        self.handler.is_connected = True
        
        # Test different error responses
        error_scenarios = [
            (401, "Invalid API key or unauthorized access"),
            (403, "API quota exceeded or forbidden access"),
            (429, "Rate limit exceeded. Please try again later"),
            (500, "API request failed with status 500")
        ]
        
        for status_code, expected_message in error_scenarios:
            with self.subTest(status_code=status_code):
                mock_response = Mock()
                mock_response.status_code = status_code
                mock_response.text = f"Error {status_code}"
                mock_get.return_value = mock_response
                
                with self.assertRaises(Exception) as context:
                    self.handler.call_coinmarketcap_api('/v1/test')
                
                self.assertIn(expected_message.lower(), str(context.exception).lower())

    @patch('requests.get')
    def test_call_api_network_exceptions(self, mock_get):
        """Test API call with various network exceptions"""
        self.handler.is_connected = True
        
        exceptions = [
            (requests.exceptions.Timeout("Timeout"), "API request timed out"),
            (requests.exceptions.ConnectionError("Connection failed"), "Failed to connect to CoinMarketCap API"),
            (requests.exceptions.RequestException("General error"), "API request failed")
        ]
        
        for exception, expected_message in exceptions:
            with self.subTest(exception=type(exception).__name__):
                mock_get.side_effect = exception
                
                with self.assertRaises(Exception) as context:
                    self.handler.call_coinmarketcap_api('/v1/test')
                
                self.assertIn(expected_message.lower(), str(context.exception).lower())

    def test_get_api_key_info_success(self):
        """Test get_api_key_info success"""
        sample_response = {'data': {'plan': {'name': 'Basic'}}}
        
        with patch.object(self.handler, 'call_coinmarketcap_api', return_value=sample_response):
            result = self.handler.get_api_key_info()
            self.assertEqual(result, sample_response)

    def test_get_api_key_info_failure(self):
        """Test get_api_key_info failure"""
        with patch.object(self.handler, 'call_coinmarketcap_api', side_effect=Exception("API Error")):
            with self.assertRaises(Exception):
                self.handler.get_api_key_info()


class TestAllTables(unittest.TestCase):
    """Comprehensive tests for all table classes"""
    
    def setUp(self):
        """Set up test fixtures for all tables"""
        self.mock_handler = Mock()
        
        # Sample responses for different endpoints
        self.sample_responses = {
            'listings': {
                'data': [
                    {
                        'id': 1, 'name': 'Bitcoin', 'symbol': 'BTC', 'slug': 'bitcoin',
                        'cmc_rank': 1, 'circulating_supply': 19500000,
                        'total_supply': 19500000, 'max_supply': 21000000,
                        'quote': {
                            'USD': {
                                'price': 45000.50, 'volume_24h': 25000000000,
                                'market_cap': 877500000000, 'percent_change_1h': -0.5,
                                'percent_change_24h': 2.3, 'percent_change_7d': -1.2,
                                'last_updated': '2025-06-03T10:00:00.000Z'
                            }
                        }
                    }
                ]
            },
            'quotes': {
                'data': {
                    'BTC': {
                        'id': 1, 'name': 'Bitcoin', 'symbol': 'BTC',
                        'quote': {
                            'USD': {
                                'price': 45000.50, 'volume_24h': 25000000000,
                                'market_cap': 877500000000, 'percent_change_1h': -0.5,
                                'percent_change_24h': 2.3, 'percent_change_7d': -1.2,
                                'last_updated': '2025-06-03T10:00:00.000Z'
                            }
                        }
                    }
                }
            },
            'info': {
                'data': {
                    'BTC': {
                        'id': 1, 'name': 'Bitcoin', 'symbol': 'BTC',
                        'category': 'coin', 'description': 'Bitcoin description',
                        'urls': {
                            'website': ['https://bitcoin.org/'],
                            'technical_doc': ['https://bitcoin.org/bitcoin.pdf'],
                            'twitter': ['https://twitter.com/bitcoin'],
                            'reddit': ['https://reddit.com/r/bitcoin']
                        },
                        'date_added': '2013-04-28T00:00:00.000Z'
                    }
                }
            },
            'global_metrics': {
                'data': {
                    'active_cryptocurrencies': 9000,
                    'total_cryptocurrencies': 15000,
                    'active_market_pairs': 80000,
                    'active_exchanges': 400,
                    'quote': {
                        'USD': {
                            'total_market_cap': 2000000000000,
                            'total_volume_24h': 100000000000,
                            'bitcoin_percentage_of_market_cap': 45.5,
                            'altcoin_percentage_of_market_cap': 54.5,
                            'altcoin_market_cap': 1090000000000,
                            'last_updated': '2025-06-03T10:00:00.000Z'
                        }
                    }
                }
            }
        }

    def create_mock_query(self, where_conditions=None, limit_value=None):
        """Helper to create mock query objects"""
        mock_query = Mock()
        mock_query.where = Mock() if where_conditions else None
        mock_query.limit = Mock() if limit_value else None
        if limit_value:
            mock_query.limit.value = limit_value
        return mock_query

    def test_listings_table_comprehensive(self):
        """Comprehensive test for ListingTable"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['listings']
        table = ListingTable(self.mock_handler)
        
        # Test get_columns
        columns_result = table.get_columns()
        self.assertEqual(columns_result.type, RESPONSE_TYPE.TABLE)
        
        # Test basic select
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions', return_value=[]):
            mock_query = self.create_mock_query()
            result = table.select(mock_query)
            
            self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
            self.assertIsInstance(result.data_frame, pd.DataFrame)

    def test_listings_table_with_conditions(self):
        """Test ListingTable with various WHERE conditions"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['listings']
        table = ListingTable(self.mock_handler)
        
        # Test with limit condition
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions', 
                  return_value=[('=', 'limit', '50')]):
            mock_query = self.create_mock_query(where_conditions=True)
            result = table.select(mock_query)
            
            # Verify API was called with correct parameters
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['limit'], 50)

    def test_listings_table_with_query_limit(self):
        """Test ListingTable with LIMIT clause"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['listings']
        table = ListingTable(self.mock_handler)
        
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions', return_value=[]):
            mock_query = self.create_mock_query(limit_value=25)
            result = table.select(mock_query)
            
            # Verify limit was applied
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['limit'], 25)

    def test_quotes_table_comprehensive(self):
        """Comprehensive test for QuotesTable"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['quotes']
        table = QuotesTable(self.mock_handler)
        
        # Test get_columns
        columns_result = table.get_columns()
        self.assertEqual(columns_result.type, RESPONSE_TYPE.TABLE)
        
        # Test with symbol condition
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions',
                  return_value=[('=', 'symbol', 'BTC')]):
            mock_query = self.create_mock_query(where_conditions=True)
            result = table.select(mock_query)
            
            self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['symbol'], 'BTC')

    def test_quotes_table_with_id_condition(self):
        """Test QuotesTable with ID condition"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['quotes']
        table = QuotesTable(self.mock_handler)
        
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions',
                  return_value=[('=', 'id', '1')]):
            mock_query = self.create_mock_query(where_conditions=True)
            result = table.select(mock_query)
            
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['id'], '1')

    def test_quotes_table_default_symbols(self):
        """Test QuotesTable uses default symbols when no conditions"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['quotes']
        table = QuotesTable(self.mock_handler)
        
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions', return_value=[]):
            mock_query = self.create_mock_query()
            result = table.select(mock_query)
            
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertIn('symbol', call_args)
            self.assertIn('BTC', call_args['symbol'])

    def test_info_table_comprehensive(self):
        """Comprehensive test for InfoTable"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['info']
        table = InfoTable(self.mock_handler)
        
        # Test get_columns
        columns_result = table.get_columns()
        self.assertEqual(columns_result.type, RESPONSE_TYPE.TABLE)
        
        # Test with symbol condition
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions',
                  return_value=[('=', 'symbol', 'BTC')]):
            mock_query = self.create_mock_query(where_conditions=True)
            result = table.select(mock_query)
            
            self.assertEqual(result.type, RESPONSE_TYPE.TABLE)

    def test_info_table_with_id_condition(self):
        """Test InfoTable with ID condition"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['info']
        table = InfoTable(self.mock_handler)
        
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions',
                  return_value=[('=', 'id', '1')]):
            mock_query = self.create_mock_query(where_conditions=True)
            result = table.select(mock_query)
            
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['id'], '1')

    def test_info_table_default_symbols(self):
        """Test InfoTable uses default symbols when no conditions"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['info']
        table = InfoTable(self.mock_handler)
        
        with patch('mindsdb.integrations.utilities.sql_utils.extract_comparison_conditions', return_value=[]):
            mock_query = self.create_mock_query()
            result = table.select(mock_query)
            
            call_args = self.mock_handler.call_coinmarketcap_api.call_args[0][1]
            self.assertEqual(call_args['symbol'], 'BTC,ETH')  # Default

    def test_global_metrics_table_comprehensive(self):
        """Comprehensive test for GlobalMetricsTable"""
        self.mock_handler.call_coinmarketcap_api.return_value = self.sample_responses['global_metrics']
        table = GlobalMetricsTable(self.mock_handler)
        
        # Test get_columns
        columns_result = table.get_columns()
        self.assertEqual(columns_result.type, RESPONSE_TYPE.TABLE)
        
        # Test select (no conditions needed for global metrics)
        mock_query = self.create_mock_query()
        result = table.select(mock_query)
        
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 1)  # Global metrics returns single row

    def test_all_tables_error_handling(self):
        """Test error handling for all tables"""
        tables = [
            ('listings', ListingTable),
            ('quotes', QuotesTable),
            ('info', InfoTable),
            ('global_metrics', GlobalMetricsTable)
        ]
        
        for table_name, table_class in tables:
            with self.subTest(table=table_name):
                # Mock handler that raises exception
                error_handler = Mock()
                error_handler.call_coinmarketcap_api.side_effect = Exception("API Error")
                
                table = table_class(error_handler)
                mock_query = self.create_mock_query()
                
                result = table.select(mock_query)
                
                self.assertEqual(result.type, RESPONSE_TYPE.ERROR)
                self.assertIn("API Error", result.error_message)

    def test_all_tables_get_columns_error_handling(self):
        """Test get_columns error handling for all tables"""
        tables = [ListingTable, QuotesTable, InfoTable, GlobalMetricsTable]
        
        for table_class in tables:
            with self.subTest(table=table_class.__name__):
                table = table_class(Mock())
                
                # This should not raise an exception
                result = table.get_columns()
                self.assertEqual(result.type, RESPONSE_TYPE.TABLE)

    def test_data_parsing_methods(self):
        """Test all data parsing methods"""
        # Test ListingTable parsing
        listings_table = ListingTable(Mock())
        df = listings_table._parse_listings_data(self.sample_responses['listings'])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]['name'], 'Bitcoin')
        
        # Test QuotesTable parsing
        quotes_table = QuotesTable(Mock())
        df = quotes_table._parse_quotes_data(self.sample_responses['quotes'])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        
        # Test InfoTable parsing
        info_table = InfoTable(Mock())
        df = info_table._parse_info_data(self.sample_responses['info'])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        
        # Test GlobalMetricsTable parsing
        global_table = GlobalMetricsTable(Mock())
        df = global_table._parse_global_data(self.sample_responses['global_metrics'])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)


if __name__ == '__main__':
    unittest.main()
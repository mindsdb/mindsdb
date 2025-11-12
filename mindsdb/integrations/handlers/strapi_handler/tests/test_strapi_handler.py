import unittest
from unittest.mock import patch, Mock
import pandas as pd
from mindsdb.integrations.handlers.strapi_handler.strapi_handler import StrapiHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class StrapiHandlerTest(unittest.TestCase):

    def setUp(self):
        self.connection_data = {
            'host': 'localhost',
            'port': '1337',
            'api_token': 'test_token_123',
            'plural_api_ids': ['products', 'sellers']
        }
        self.handler = StrapiHandler(name='myshop', connection_data=self.connection_data)
        
        # Mock data for testing (matching real Strapi API response structure)
        self.mock_products_data = [
            {
                'id': 45,
                'documentId': 'mvaprjyy72ayx7z4v592sdnr',
                'title': 'Mens Casual Premium Slim Fit T-Shirts',
                'desc': 'Slim-fitting style, contrast raglan long sleeve, lightweight & breathable fabric.',
                'price': 22.3,
                'createdAt': '2025-09-09T08:57:55.574Z',
                'updatedAt': '2025-09-09T09:53:41.392Z',
                'publishedAt': '2025-09-09T09:53:41.412Z'
            },
            {
                'id': 46,
                'documentId': 'abc123def456ghi789',
                'title': 'Womens Cotton Jacket',
                'desc': 'Great outerwear for Spring/Autumn/Winter.',
                'price': 55.99,
                'createdAt': '2025-09-09T08:58:55.574Z',
                'updatedAt': '2025-09-09T09:54:41.392Z',
                'publishedAt': '2025-09-09T09:54:41.412Z'
            }
        ]
        
        self.mock_sellers_data = [
            {
                'id': 1,
                'documentId': 'seller123',
                'name': 'Test Seller',
                'email': 'seller@test.com',
                'sellerid': 'seller001',
                'createdAt': '2025-09-09T08:57:55.574Z',
                'updatedAt': '2025-09-09T09:53:41.392Z',
                'publishedAt': '2025-09-09T09:53:41.412Z'
            }
        ]

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.get')
    def test_0_check_connection(self, mock_get):
        # Mock successful connection response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'data': {'name': 'test-strapi', 'version': '4.0.0'}}
        mock_get.return_value = mock_response
        
        # Ensure the connection is successful
        self.assertTrue(self.handler.check_connection())

    def test_1_get_table(self):
        # Mock the plural_api_ids from connection data
        result = self.handler.get_tables()
        self.assertIsNotNone(result)
        assert result is not RESPONSE_TYPE.ERROR

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.get')
    def test_2_get_columns(self, mock_get):
        # Mock response for schema fetching (single record with limit=1)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': [self.mock_products_data[0]]  # Return first product for schema discovery
        }
        mock_get.return_value = mock_response
        
        result = self.handler.get_columns('products')
        assert result is not RESPONSE_TYPE.ERROR

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.get')
    def test_3_get_data(self, mock_get):
        # Mock responses: first call for schema (limit=1), second call for actual data
        schema_response = Mock()
        schema_response.status_code = 200
        schema_response.json.return_value = {'data': [self.mock_products_data[0]]}
        
        data_response = Mock()
        data_response.status_code = 200
        data_response.json.return_value = {'data': self.mock_products_data}
        
        # Return schema response first, then data response
        mock_get.side_effect = [schema_response, data_response]
        
        # Ensure that you can retrieve data from a table
        data = self.handler.native_query('SELECT * FROM products')
        assert data.type is not RESPONSE_TYPE.ERROR

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.get')
    def test_4_get_data_with_condition(self, mock_get):
        # Mock responses: first call for schema (limit=1), second call for specific documentId
        schema_response = Mock()
        schema_response.status_code = 200
        schema_response.json.return_value = {'data': [self.mock_products_data[0]]}
        
        specific_response = Mock()
        specific_response.status_code = 200
        specific_response.json.return_value = {
            'data': self.mock_products_data[0]  # Return single product (not in array for specific ID)
        }
        
        # Return schema response first, then specific product response
        mock_get.side_effect = [schema_response, specific_response]
        
        # Ensure that you can retrieve data with a condition
        data = self.handler.native_query("SELECT * FROM products WHERE documentId = 'mvaprjyy72ayx7z4v592sdnr'")
        assert data.type is not RESPONSE_TYPE.ERROR

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.request')
    def test_5_insert_data(self, mock_request):
        # Mock response for successful data insertion
        mock_response = Mock()
        mock_response.status_code = 201
        mock_response.json.return_value = {
            'data': {
                'id': 2,
                'documentId': 'newdocid123',
                'name': 'Ram',
                'email': 'ram@gmail.com',
                'sellerid': 'ramu4',
                'createdAt': '2025-09-09T08:57:55.574Z',
                'updatedAt': '2025-09-09T09:53:41.392Z',
                'publishedAt': '2025-09-09T09:53:41.412Z'
            }
        }
        mock_request.return_value = mock_response
        
        # Ensure that data insertion is successful
        query = "INSERT INTO myshop.sellers (name, email, sellerid) VALUES ('Ram', 'ram@gmail.com', 'ramu4')"
        result = self.handler.native_query(query)
        self.assertIsNotNone(result)
        assert result.type is not RESPONSE_TYPE.ERROR

    @patch('mindsdb.integrations.handlers.strapi_handler.strapi_handler.requests.request')
    def test_6_update_data(self, mock_request):
        # Mock response for successful data update
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'data': {
                'id': 45,
                'documentId': 'mvaprjyy72ayx7z4v592sdnr',
                'title': 'Updated Product Title',  # Updated title
                'desc': 'Slim-fitting style, contrast raglan long sleeve, lightweight & breathable fabric.',
                'price': 22.3,
                'createdAt': '2025-09-09T08:57:55.574Z',
                'updatedAt': '2025-09-09T09:53:41.392Z',
                'publishedAt': '2025-09-09T09:53:41.412Z'
            }
        }
        mock_request.return_value = mock_response
        
        # Ensure that data updating is successful
        query = "UPDATE products SET title = 'Updated Product Title' WHERE documentId = 'mvaprjyy72ayx7z4v592sdnr'"
        result = self.handler.native_query(query)
        self.assertIsNotNone(result)
        assert result.type is not RESPONSE_TYPE.ERROR


if __name__ == '__main__':
    unittest.main()

import unittest
from unittest.mock import patch, MagicMock
from mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_handler import BigCommerceHandler
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE
from mindsdb_sql_parser import ast


class BigCommerceHandlerTest(unittest.TestCase):
    def setUp(self):
        self.handler = BigCommerceHandler(
            "test_bigcommerce_handler",
            connection_data={
                "api_base": "https://api.bigcommerce.com/stores/test-store/v3/",
                "access_token": "mock_access_token",
            },
        )
        self.patcher = patch(
            "mindsdb.integrations.handlers.bigcommerce_handler.bigcommerce_handler.BigCommerceAPIClient"
        )
        self.mock_client = self.patcher.start()
        self.mock_client_instance = MagicMock()
        self.mock_client.return_value = self.mock_client_instance

    def tearDown(self):
        self.patcher.stop()

    def test_check_connection_success(self):
        """Test successful connection to BigCommerce API"""
        self.mock_client_instance.get_products.return_value = [{"id": 1, "name": "Test Product"}]
        response = self.handler.check_connection()
        self.assertTrue(response.success)
        self.mock_client_instance.get_products.assert_called_once_with(limit=1)

    def test_check_connection_failure(self):
        """Test failed connection to BigCommerce API"""
        self.mock_client_instance.get_products.side_effect = Exception("Connection failed")
        response = self.handler.check_connection()
        self.assertFalse(response.success)
        self.assertIsNotNone(response.error_message)

    def test_get_tables(self):
        """Test retrieving list of tables"""
        result = self.handler.get_tables()
        self.assertIsNotNone(result)
        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        # Check that all expected tables are present
        table_names = [row[0] for row in result.data_frame.values]
        expected_tables = [
            "orders",
            "products",
            "customers",
            "categories",
            "pickups",
            "promotions",
            "wishlists",
            "segments",
            "brands",
        ]
        for table in expected_tables:
            self.assertIn(table, table_names)

    def test_query_products(self):
        """Test querying products table"""
        mock_products = [
            {
                "id": 1,
                "name": "Product 1",
                "type": "physical",
                "sku": "SKU-001",
                "price": "29.99",
                "inventory_level": 100,
            },
            {
                "id": 2,
                "name": "Product 2",
                "type": "digital",
                "sku": "SKU-002",
                "price": "49.99",
                "inventory_level": 50,
            },
        ]
        self.mock_client_instance.get_products.return_value = mock_products

        query = ast.Select(targets=[ast.Star()], from_table=ast.Identifier("products"), limit=ast.Constant(10))

        self.handler.connect = MagicMock(return_value=self.mock_client_instance)
        result = self.handler.query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_query_orders(self):
        """Test querying orders table"""
        mock_orders = [
            {"id": 100, "customer_id": 1, "status": "completed", "total_inc_tax": 99.99, "date_created": "2024-01-15"},
            {"id": 101, "customer_id": 2, "status": "pending", "total_inc_tax": 149.99, "date_created": "2024-01-16"},
        ]
        self.mock_client_instance.get_orders.return_value = mock_orders

        query = ast.Select(targets=[ast.Star()], from_table=ast.Identifier("orders"), limit=ast.Constant(10))

        self.handler.connect = MagicMock(return_value=self.mock_client_instance)
        result = self.handler.query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_query_customers(self):
        """Test querying customers table"""
        mock_customers = [
            {
                "id": 1,
                "email": "customer1@example.com",
                "first_name": "John",
                "last_name": "Doe",
                "customer_group_id": 0,
            },
            {
                "id": 2,
                "email": "customer2@example.com",
                "first_name": "Jane",
                "last_name": "Smith",
                "customer_group_id": 1,
            },
        ]
        self.mock_client_instance.get_customers.return_value = mock_customers

        query = ast.Select(targets=[ast.Star()], from_table=ast.Identifier("customers"), limit=ast.Constant(10))

        self.handler.connect = MagicMock(return_value=self.mock_client_instance)
        result = self.handler.query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(len(result.data_frame), 2)

    def test_query_with_filter(self):
        """Test querying with WHERE clause"""
        mock_products = [{"id": 1, "name": "Filtered Product", "price": 29.99}]
        self.mock_client_instance.get_products.return_value = mock_products

        query = ast.Select(
            targets=[ast.Star()],
            from_table=ast.Identifier("products"),
            where=ast.BinaryOperation("=", args=[ast.Identifier("id"), ast.Constant(1)]),
            limit=ast.Constant(10),
        )

        self.handler.connect = MagicMock(return_value=self.mock_client_instance)
        result = self.handler.query(query)

        self.assertEqual(result.type, RESPONSE_TYPE.TABLE)
        self.assertGreaterEqual(len(result.data_frame), 0)

    def test_connect_missing_parameters(self):
        """Test connection with missing required parameters"""
        handler = BigCommerceHandler(
            "test_handler",
            connection_data={
                "api_base": "https://api.bigcommerce.com/stores/test/"
                # Missing access_token
            },
        )

        with self.assertRaises(ValueError) as context:
            handler.connect()

        self.assertIn("Required parameters", str(context.exception))

    def test_meta_columns(self):
        """Test retrieving metadata columns"""
        meta_columns = self.handler.meta_get_columns()
        self.assertTrue(len(meta_columns.data_frame) > 0)


if __name__ == "__main__":
    unittest.main()

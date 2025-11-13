import unittest
from unittest.mock import MagicMock, patch
import sys

from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.integrations.libs.api_handler_exceptions import (
    MissingConnectionParams,
    ConnectionFailed,
    InvalidNativeQuery,
)

# Mock shopify and requests modules before importing the handler
if "shopify" not in sys.modules:
    sys.modules["shopify"] = MagicMock()
    sys.modules["shopify.ShopifyResource"] = MagicMock()
    sys.modules["shopify.Session"] = MagicMock()
    sys.modules["shopify.Shop"] = MagicMock()

if "requests" not in sys.modules:
    sys.modules["requests"] = MagicMock()

from mindsdb.integrations.handlers.shopify_handler.shopify_handler import ShopifyHandler


class BaseShopifyHandlerTest(unittest.TestCase):
    """Base test class with common setup and helper methods."""

    # Test constants
    TEST_SHOP_URL = "test-shop.myshopify.com"
    TEST_CLIENT_ID = "test_client_id"
    TEST_CLIENT_SECRET = "test_client_secret"
    TEST_HANDLER_NAME = "test_shopify_handler"

    def setUp(self):
        """Set up test fixtures."""
        self.connection_data = {
            "shop_url": self.TEST_SHOP_URL,
            "client_id": self.TEST_CLIENT_ID,
            "client_secret": self.TEST_CLIENT_SECRET,
        }

    def tearDown(self):
        """Clean up after tests."""
        pass


class TestShopifyHandlerInitialization(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler initialization."""

    def test_handler_initialization_success(self):
        """Test successful handler initialization with all required parameters."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        self.assertEqual(handler.name, self.TEST_HANDLER_NAME)
        self.assertEqual(handler.connection_data, self.connection_data)
        self.assertFalse(handler.is_connected)
        self.assertIsNone(handler.connection)

    def test_handler_initialization_without_connection_data(self):
        """Test handler initialization fails when connection_data is missing."""
        with self.assertRaises(MissingConnectionParams) as context:
            ShopifyHandler(self.TEST_HANDLER_NAME)

        self.assertIn("Incomplete parameters", str(context.exception))

    def test_handler_initialization_missing_shop_url(self):
        """Test handler initialization fails when shop_url is missing."""
        incomplete_data = {
            "client_id": self.TEST_CLIENT_ID,
            "client_secret": self.TEST_CLIENT_SECRET,
        }

        with self.assertRaises(MissingConnectionParams) as context:
            ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=incomplete_data)

        self.assertIn("shop_url", str(context.exception))

    def test_handler_initialization_missing_client_id(self):
        """Test handler initialization fails when client_id is missing."""
        incomplete_data = {
            "shop_url": self.TEST_SHOP_URL,
            "client_secret": self.TEST_CLIENT_SECRET,
        }

        with self.assertRaises(MissingConnectionParams) as context:
            ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=incomplete_data)

        self.assertIn("client_id", str(context.exception))

    def test_handler_initialization_missing_client_secret(self):
        """Test handler initialization fails when client_secret is missing."""
        incomplete_data = {
            "shop_url": self.TEST_SHOP_URL,
            "client_id": self.TEST_CLIENT_ID,
        }

        with self.assertRaises(MissingConnectionParams) as context:
            ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=incomplete_data)

        self.assertIn("client_secret", str(context.exception))

    def test_handler_tables_registered(self):
        """Test that all required tables are registered during initialization."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        expected_tables = [
            "products",
            "customers",
            "orders",
            "product_variants",
            "marketing_events",
            "inventory_items",
            "staff_members",
            "gift_cards",
        ]

        for table_name in expected_tables:
            self.assertIn(table_name, handler._tables)


class TestShopifyHandlerConnection(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler connection management."""

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_connect_success(self, mock_shopify, mock_requests):
        """Test successful connection to Shopify API."""
        # Mock the OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock the Session
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        result = handler.connect()

        # Verify OAuth request was made
        mock_requests.post.assert_called_once()
        call_args = mock_requests.post.call_args
        self.assertIn(self.TEST_SHOP_URL, call_args[0][0])
        self.assertEqual(call_args[1]["data"]["client_id"], self.TEST_CLIENT_ID)
        self.assertEqual(call_args[1]["data"]["client_secret"], self.TEST_CLIENT_SECRET)

        # Verify session was created
        mock_shopify.Session.assert_called_once_with(
            self.TEST_SHOP_URL, "2025-10", "test_access_token"
        )

        self.assertTrue(handler.is_connected)
        self.assertEqual(result, mock_session)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_connect_when_already_connected(self, mock_shopify, mock_requests):
        """Test that connect returns existing connection when already connected."""
        mock_session = MagicMock()

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        handler.connection = mock_session
        handler.is_connected = True

        result = handler.connect()

        # Should not make new OAuth request
        mock_requests.post.assert_not_called()
        mock_shopify.Session.assert_not_called()

        self.assertEqual(result, mock_session)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    def test_connect_oauth_failure(self, mock_requests):
        """Test connection failure when OAuth request fails."""
        error_msg = "Invalid credentials"
        mock_requests.post.side_effect = Exception(error_msg)

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        with self.assertRaises(Exception) as context:
            handler.connect()

        self.assertIn(error_msg, str(context.exception))
        self.assertFalse(handler.is_connected)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    def test_connect_missing_access_token(self, mock_requests):
        """Test connection failure when access token is missing from response."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}  # No access_token
        mock_requests.post.return_value = mock_response

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        with self.assertRaises(ConnectionFailed) as context:
            handler.connect()

        self.assertIn("Unable to get an access token", str(context.exception))
        self.assertFalse(handler.is_connected)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_check_connection_success(self, mock_shopify, mock_requests):
        """Test successful connection check."""
        # Mock OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock session and shop
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session
        mock_shopify.ShopifyResource.activate_session = MagicMock()
        mock_shopify.Shop.current.return_value = MagicMock()

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        result = handler.check_connection()

        self.assertIsInstance(result, StatusResponse)
        self.assertTrue(result.success)
        self.assertTrue(handler.is_connected)

        # Verify session was activated and Shop.current was called
        mock_shopify.ShopifyResource.activate_session.assert_called_once_with(mock_session)
        mock_shopify.Shop.current.assert_called_once()

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_check_connection_failure(self, mock_shopify, mock_requests):
        """Test connection check failure."""
        # Mock OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock session
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session
        mock_shopify.ShopifyResource.activate_session = MagicMock()

        # Make Shop.current fail
        error_message = "Invalid shop"
        mock_shopify.Shop.current.side_effect = Exception(error_message)

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        with self.assertRaises(ConnectionFailed):
            handler.check_connection()

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    def test_check_connection_oauth_failure(self, mock_requests):
        """Test connection check failure during OAuth."""
        error_message = "OAuth failed"
        mock_requests.post.side_effect = Exception(error_message)

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        with self.assertRaises(ConnectionFailed):
            handler.check_connection()


class TestShopifyHandlerQueries(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler query execution."""

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.parse_sql")
    def test_native_query_success(self, mock_parse_sql, mock_shopify, mock_requests):
        """Test successful native query execution."""
        # Mock OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock session
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session

        # Mock parse_sql
        mock_ast = MagicMock()
        mock_parse_sql.return_value = mock_ast

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        # Mock the query method to return a success response
        handler.query = MagicMock()
        mock_query_response = MagicMock()
        handler.query.return_value = mock_query_response

        query = "SELECT * FROM products"
        result = handler.native_query(query)

        mock_parse_sql.assert_called_once_with(query)
        handler.query.assert_called_once_with(mock_ast)
        self.assertEqual(result, mock_query_response)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.parse_sql")
    def test_native_query_invalid_sql(self, mock_parse_sql):
        """Test native query with invalid SQL."""
        mock_parse_sql.side_effect = Exception("Invalid SQL")

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        query = "INVALID SQL QUERY"
        with self.assertRaises(InvalidNativeQuery) as context:
            handler.native_query(query)

        self.assertIn("invalid", str(context.exception).lower())


class TestShopifyHandlerConnectionArgs(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler connection arguments validation."""

    def test_connection_args_structure(self):
        """Test that connection_args has the correct structure."""
        from mindsdb.integrations.handlers.shopify_handler.connection_args import connection_args

        required_fields = ["type", "description", "required", "label"]

        # Check shop_url
        self.assertIn("shop_url", connection_args)
        for field in required_fields:
            self.assertIn(field, connection_args["shop_url"])
        self.assertTrue(connection_args["shop_url"]["required"])

        # Check client_id
        self.assertIn("client_id", connection_args)
        for field in required_fields:
            self.assertIn(field, connection_args["client_id"])
        self.assertTrue(connection_args["client_id"]["required"])

        # Check client_secret
        self.assertIn("client_secret", connection_args)
        for field in required_fields:
            self.assertIn(field, connection_args["client_secret"])
        self.assertTrue(connection_args["client_secret"]["required"])
        self.assertTrue(connection_args["client_secret"].get("secret", False))

    def test_connection_args_example_structure(self):
        """Test that connection_args_example has the correct structure."""
        from mindsdb.integrations.handlers.shopify_handler.connection_args import (
            connection_args_example,
        )

        self.assertIn("shop_url", connection_args_example)
        self.assertIn("client_id", connection_args_example)
        self.assertIn("client_secret", connection_args_example)

        self.assertIsInstance(connection_args_example["shop_url"], str)
        self.assertIsInstance(connection_args_example["client_id"], str)
        self.assertIsInstance(connection_args_example["client_secret"], str)


class TestShopifyHandlerEdgeCases(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler edge cases."""

    def test_handler_name_attribute(self):
        """Test that handler has correct name attribute."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        self.assertEqual(handler.name, self.TEST_HANDLER_NAME)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_multiple_connections(self, mock_shopify, mock_requests):
        """Test multiple connection attempts."""
        # Mock OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock session
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        # First connection
        result1 = handler.connect()
        self.assertTrue(handler.is_connected)

        # Second connection (should return existing)
        result2 = handler.connect()
        self.assertEqual(result1, result2)

        # Should only call OAuth once
        self.assertEqual(mock_requests.post.call_count, 1)

    def test_connection_data_preserved(self):
        """Test that connection data is preserved after initialization."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        self.assertEqual(handler.connection_data["shop_url"], self.TEST_SHOP_URL)
        self.assertEqual(handler.connection_data["client_id"], self.TEST_CLIENT_ID)
        self.assertEqual(handler.connection_data["client_secret"], self.TEST_CLIENT_SECRET)

    def test_empty_connection_data(self):
        """Test handler initialization with empty connection_data."""
        with self.assertRaises(MissingConnectionParams):
            ShopifyHandler(self.TEST_HANDLER_NAME, connection_data={})

    def test_partial_connection_data(self):
        """Test handler initialization with partial connection_data."""
        partial_data = {"shop_url": self.TEST_SHOP_URL}

        with self.assertRaises(MissingConnectionParams) as context:
            ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=partial_data)

        # Should mention the missing parameters
        error_message = str(context.exception)
        self.assertTrue("client_id" in error_message or "client_secret" in error_message)


class TestShopifyHandlerIntegration(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler integration scenarios."""

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.requests")
    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_handler.shopify")
    def test_connection_and_check(self, mock_shopify, mock_requests):
        """Test connection followed by check_connection."""
        # Mock OAuth response
        mock_response = MagicMock()
        mock_response.json.return_value = {"access_token": "test_access_token"}
        mock_requests.post.return_value = mock_response

        # Mock session
        mock_session = MagicMock()
        mock_shopify.Session.return_value = mock_session
        mock_shopify.ShopifyResource.activate_session = MagicMock()
        mock_shopify.Shop.current.return_value = MagicMock()

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        # First connect
        handler.connect()
        self.assertTrue(handler.is_connected)

        # Then check connection
        result = handler.check_connection()
        self.assertTrue(result.success)

    def test_handler_with_extra_kwargs(self):
        """Test handler initialization with extra keyword arguments."""
        extra_kwargs = {
            "connection_data": self.connection_data,
            "extra_param": "extra_value",
        }

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, **extra_kwargs)
        self.assertEqual(handler.name, self.TEST_HANDLER_NAME)
        self.assertTrue(handler.is_connected is False)


class TestShopifyHandlerTableMetadata(BaseShopifyHandlerTest):
    """Test suite for Shopify Handler table metadata methods."""

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_tables.query_graphql")
    def test_products_table_meta_get_tables(self, mock_query_graphql):
        """Test meta_get_tables method for products table."""
        mock_query_graphql.return_value = {"productsCount": {"count": 100}}

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        products_table = handler._tables["products"]

        result = products_table.meta_get_tables()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_name"], "products")
        self.assertEqual(result["table_type"], "BASE TABLE")
        self.assertIn("table_description", result)
        self.assertEqual(result["row_count"], 100)

    def test_products_table_meta_get_primary_keys(self):
        """Test meta_get_primary_keys method for products table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        products_table = handler._tables["products"]

        result = products_table.meta_get_primary_keys("products")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "products")
        self.assertEqual(result[0]["column_name"], "id")

    def test_products_table_meta_get_foreign_keys(self):
        """Test meta_get_foreign_keys method for products table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        products_table = handler._tables["products"]

        result = products_table.meta_get_foreign_keys("products", ["products", "orders"])

        self.assertIsInstance(result, list)
        # Products table should have no foreign keys
        self.assertEqual(len(result), 0)

    def test_products_table_get_columns(self):
        """Test get_columns method for products table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        products_table = handler._tables["products"]

        result = products_table.get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Check that some expected columns are present
        self.assertIn("id", result)
        self.assertIn("title", result)
        # All items should be strings
        for column_name in result:
            self.assertIsInstance(column_name, str)

    def test_products_table_meta_get_columns(self):
        """Test meta_get_columns method for products table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        products_table = handler._tables["products"]

        result = products_table.meta_get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Each column should be a dictionary with metadata
        for column in result:
            self.assertIsInstance(column, dict)
            self.assertIn("COLUMN_NAME", column)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_tables.query_graphql")
    def test_product_variants_table_meta_get_tables(self, mock_query_graphql):
        """Test meta_get_tables method for product_variants table."""
        mock_query_graphql.return_value = {"productVariantsCount": {"count": 250}}

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        variants_table = handler._tables["product_variants"]

        result = variants_table.meta_get_tables()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_name"], "product_variants")
        self.assertEqual(result["table_type"], "BASE TABLE")
        self.assertIn("table_description", result)
        self.assertEqual(result["row_count"], 250)

    def test_product_variants_table_meta_get_primary_keys(self):
        """Test meta_get_primary_keys method for product_variants table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        variants_table = handler._tables["product_variants"]

        result = variants_table.meta_get_primary_keys("product_variants")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "product_variants")
        self.assertEqual(result[0]["column_name"], "id")

    def test_product_variants_table_meta_get_foreign_keys(self):
        """Test meta_get_foreign_keys method for product_variants table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        variants_table = handler._tables["product_variants"]

        result = variants_table.meta_get_foreign_keys("product_variants", ["products", "product_variants"])

        self.assertIsInstance(result, list)
        # Product variants should have a foreign key to products
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]["PARENT_TABLE_NAME"], "product_variants")
        self.assertEqual(result[0]["PARENT_COLUMN_NAME"], "productId")
        self.assertEqual(result[0]["CHILD_TABLE_NAME"], "products")
        self.assertEqual(result[0]["CHILD_COLUMN_NAME"], "id")

    def test_product_variants_table_get_columns(self):
        """Test get_columns method for product_variants table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        variants_table = handler._tables["product_variants"]

        result = variants_table.get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Check that some expected columns are present
        self.assertIn("id", result)
        self.assertIn("productId", result)

    def test_product_variants_table_meta_get_columns(self):
        """Test meta_get_columns method for product_variants table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        variants_table = handler._tables["product_variants"]

        result = variants_table.meta_get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        for column in result:
            self.assertIsInstance(column, dict)
            self.assertIn("COLUMN_NAME", column)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_tables.query_graphql")
    def test_customers_table_meta_get_tables(self, mock_query_graphql):
        """Test meta_get_tables method for customers table."""
        mock_query_graphql.return_value = {"customersCount": {"count": 500}}

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        customers_table = handler._tables["customers"]

        result = customers_table.meta_get_tables()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_name"], "customers")
        self.assertEqual(result["table_type"], "BASE TABLE")
        self.assertIn("table_description", result)
        self.assertEqual(result["row_count"], 500)

    def test_customers_table_meta_get_primary_keys(self):
        """Test meta_get_primary_keys method for customers table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        customers_table = handler._tables["customers"]

        result = customers_table.meta_get_primary_keys("customers")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "customers")
        self.assertEqual(result[0]["column_name"], "id")

    def test_customers_table_meta_get_foreign_keys(self):
        """Test meta_get_foreign_keys method for customers table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        customers_table = handler._tables["customers"]

        result = customers_table.meta_get_foreign_keys("customers", ["customers", "orders"])

        self.assertIsInstance(result, list)
        # Customers table should have no foreign keys
        self.assertEqual(len(result), 0)

    def test_customers_table_get_columns(self):
        """Test get_columns method for customers table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        customers_table = handler._tables["customers"]

        result = customers_table.get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Check that some expected columns are present
        self.assertIn("id", result)
        self.assertIn("emailAddress", result)

    def test_customers_table_meta_get_columns(self):
        """Test meta_get_columns method for customers table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        customers_table = handler._tables["customers"]

        result = customers_table.meta_get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        for column in result:
            self.assertIsInstance(column, dict)
            self.assertIn("COLUMN_NAME", column)

    @patch("mindsdb.integrations.handlers.shopify_handler.shopify_tables.query_graphql")
    def test_orders_table_meta_get_tables(self, mock_query_graphql):
        """Test meta_get_tables method for orders table."""
        mock_query_graphql.return_value = {"ordersCount": {"count": 1000}}

        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        orders_table = handler._tables["orders"]

        result = orders_table.meta_get_tables()

        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_name"], "orders")
        self.assertEqual(result["table_type"], "BASE TABLE")
        self.assertIn("table_description", result)
        self.assertEqual(result["row_count"], 1000)

    def test_orders_table_meta_get_primary_keys(self):
        """Test meta_get_primary_keys method for orders table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        orders_table = handler._tables["orders"]

        result = orders_table.meta_get_primary_keys("orders")

        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["table_name"], "orders")
        self.assertEqual(result[0]["column_name"], "id")

    def test_orders_table_meta_get_foreign_keys(self):
        """Test meta_get_foreign_keys method for orders table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        orders_table = handler._tables["orders"]

        # Test with customers in the table list
        result = orders_table.meta_get_foreign_keys("orders", ["customers", "orders"])

        self.assertIsInstance(result, list)
        # Orders table should have a foreign key to customers
        self.assertGreater(len(result), 0)
        self.assertEqual(result[0]["PARENT_TABLE_NAME"], "orders")
        self.assertEqual(result[0]["PARENT_COLUMN_NAME"], "customerId")
        self.assertEqual(result[0]["CHILD_TABLE_NAME"], "customers")
        self.assertEqual(result[0]["CHILD_COLUMN_NAME"], "id")

    def test_orders_table_get_columns(self):
        """Test get_columns method for orders table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        orders_table = handler._tables["orders"]

        result = orders_table.get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        # Check that some expected columns are present
        self.assertIn("id", result)
        self.assertIn("customerId", result)

    def test_orders_table_meta_get_columns(self):
        """Test meta_get_columns method for orders table."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)
        orders_table = handler._tables["orders"]

        result = orders_table.meta_get_columns()

        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)
        for column in result:
            self.assertIsInstance(column, dict)
            self.assertIn("COLUMN_NAME", column)

    def test_all_tables_have_metadata_methods(self):
        """Test that all registered tables have required metadata methods."""
        handler = ShopifyHandler(self.TEST_HANDLER_NAME, connection_data=self.connection_data)

        expected_tables = [
            "products",
            "customers",
            "orders",
            "product_variants",
            "marketing_events",
            "inventory_items",
            "staff_members",
            "gift_cards",
        ]

        for table_name in expected_tables:
            with self.subTest(table=table_name):
                table = handler._tables[table_name]

                # Check that all required methods exist
                self.assertTrue(hasattr(table, "meta_get_tables"))
                self.assertTrue(hasattr(table, "meta_get_primary_keys"))
                self.assertTrue(hasattr(table, "meta_get_foreign_keys"))
                self.assertTrue(hasattr(table, "get_columns"))
                self.assertTrue(hasattr(table, "meta_get_columns"))

                # Check that methods are callable
                self.assertTrue(callable(table.meta_get_tables))
                self.assertTrue(callable(table.meta_get_primary_keys))
                self.assertTrue(callable(table.meta_get_foreign_keys))
                self.assertTrue(callable(table.get_columns))
                self.assertTrue(callable(table.meta_get_columns))


if __name__ == "__main__":
    unittest.main()


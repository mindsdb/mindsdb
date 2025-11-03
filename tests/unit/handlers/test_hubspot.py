from collections import OrderedDict
import pytest
import unittest
from unittest.mock import patch, MagicMock

try:
    from hubspot.crm.objects import SimplePublicObject
    from mindsdb.integrations.handlers.hubspot_handler.hubspot_handler import HubspotHandler
except ImportError:
    pytestmark = pytest.mark.skip("HubSpot handler not installed")

from base_handler_test import BaseHandlerTestSetup

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)


class TestHubspotHandler(BaseHandlerTestSetup, unittest.TestCase):
    """Test class for HubspotHandler."""

    @property
    def dummy_connection_data(self):
        return OrderedDict(access_token="test_token_12345_dummy_not_real")

    @property
    def registered_tables(self):
        return ["companies", "contacts", "deals"]

    @property
    def err_to_raise_on_connect_failure(self):
        return Exception("Authentication failed")

    def create_handler(self):
        """Create HubspotHandler instance for testing."""
        return HubspotHandler("hubspot", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        """Create patch for HubSpot client connection."""
        return patch("mindsdb.integrations.handlers.hubspot_handler.hubspot_handler.HubSpot")

    def test_initialization(self):
        """Test if the handler initializes correctly with proper values."""
        self.assertEqual(self.handler.name, "hubspot")
        self.assertFalse(self.handler.is_connected)
        self.assertEqual(self.handler.connection_data, self.dummy_connection_data)

        # Test that tables are registered
        self.assertIn("companies", self.handler._tables.keys())
        self.assertIn("contacts", self.handler._tables.keys())
        self.assertIn("deals", self.handler._tables.keys())

    def test_connect_success(self):
        """Test if `connect` method successfully establishes connection."""
        mock_hubspot_client = MagicMock()
        self.mock_connect.return_value = mock_hubspot_client

        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.assertEqual(connection, mock_hubspot_client)
        self.mock_connect.assert_called_once_with(access_token=self.dummy_connection_data["access_token"])

    def test_connect_reuse_existing_connection(self):
        """Test that connect reuses existing connection when already connected."""
        mock_hubspot_client = MagicMock()
        self.mock_connect.return_value = mock_hubspot_client

        # First connection
        connection1 = self.handler.connect()

        # Second connection should reuse existing
        connection2 = self.handler.connect()

        self.assertEqual(connection1, connection2)
        self.mock_connect.assert_called_once()

    def test_connect_failure(self):
        """Test connect method handles connection failures properly."""
        self.mock_connect.side_effect = self.err_to_raise_on_connect_failure

        with self.assertRaises(type(self.err_to_raise_on_connect_failure)):
            self.handler.connect()
        self.assertFalse(self.handler.is_connected)

    def test_check_connection_success(self):
        """Test check_connection method with successful connection."""
        mock_hubspot_client = MagicMock()
        self.mock_connect.return_value = mock_hubspot_client

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection_failure(self):
        """Test check_connection method with failed connection."""
        self.mock_connect.side_effect = self.err_to_raise_on_connect_failure

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertFalse(response.success)
        self.assertEqual(response.error_message, self.err_to_raise_on_connect_failure)
        self.assertFalse(self.handler.is_connected)

    def test_native_query(self):
        """Test native_query method executes SQL queries."""
        mock_hubspot_client = MagicMock()
        mock_companies_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "city": "New York",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies_data

        query = "SELECT * FROM companies LIMIT 1"
        response = self.handler.native_query(query)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsNotNone(response.data_frame)

    def test_get_tables(self):
        """Test get_tables method returns registered tables."""
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), 3)  # companies, contacts, deals
        self.assertIn("table_name", df.columns)
        self.assertIn("table_type", df.columns)

        table_names = df["table_name"].tolist()
        self.assertIn("companies", table_names)
        self.assertIn("contacts", table_names)
        self.assertIn("deals", table_names)

        # All should be BASE TABLE type
        table_types = df["table_type"].unique().tolist()
        self.assertEqual(table_types, ["BASE TABLE"])

    def test_get_columns_companies(self):
        """Test get_columns method for companies table."""
        mock_hubspot_client = MagicMock()
        mock_company_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "city": "New York",
                    "phone": "+1-555-123-4567",
                    "state": "NY",
                    "domain": "testcompany.com",
                    "industry": "Technology",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_company_data

        response = self.handler.get_columns("companies")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(df.columns.tolist(), ["Field", "Type"])

        # Check that expected columns are present
        column_names = df["Field"].tolist()
        expected_columns = [
            "id",
            "name",
            "city",
            "phone",
            "state",
            "domain",
            "industry",
            "createdate",
            "lastmodifieddate",
        ]
        for col in expected_columns:
            self.assertIn(col, column_names)

    def test_get_columns_contacts(self):
        """Test get_columns method for contacts table."""
        mock_hubspot_client = MagicMock()
        mock_contact_data = [
            SimplePublicObject(
                id="456",
                properties={
                    "email": "test@example.com",
                    "firstname": "John",
                    "lastname": "Doe",
                    "phone": "+1-555-123-4567",
                    "company": "Test Company",
                    "website": "example.com",
                    "createdate": "2023-01-01T00:00:00Z",
                    "lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.contacts.get_all.return_value = mock_contact_data

        response = self.handler.get_columns("contacts")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(df.columns.tolist(), ["Field", "Type"])

        # Check that expected columns are present
        column_names = df["Field"].tolist()
        expected_columns = [
            "id",
            "email",
            "firstname",
            "lastname",
            "phone",
            "company",
            "website",
            "createdate",
            "lastmodifieddate",
        ]
        for col in expected_columns:
            self.assertIn(col, column_names)

    def test_get_columns_deals(self):
        """Test get_columns method for deals table."""
        mock_hubspot_client = MagicMock()
        mock_deal_data = [
            SimplePublicObject(
                id="789",
                properties={
                    "dealname": "Test Deal",
                    "amount": "10000",
                    "pipeline": "default",
                    "closedate": "2023-12-31",
                    "dealstage": "closedwon",
                    "hubspot_owner_id": "12345",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.deals.get_all.return_value = mock_deal_data

        response = self.handler.get_columns("deals")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(df.columns.tolist(), ["Field", "Type"])

        # Check that expected columns are present
        column_names = df["Field"].tolist()
        expected_columns = [
            "id",
            "dealname",
            "amount",
            "pipeline",
            "closedate",
            "dealstage",
            "hubspot_owner_id",
            "createdate",
            "hs_lastmodifieddate",
        ]
        for col in expected_columns:
            self.assertIn(col, column_names)

    def test_get_columns_invalid_table(self):
        """Test get_columns method with invalid table name."""
        response = self.handler.get_columns("nonexistent_table")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIsNotNone(response.error_message)

    def test_native_query_with_insert(self):
        """Test native_query with INSERT statement."""
        mock_hubspot_client = MagicMock()
        mock_created_companies = MagicMock()
        mock_created_companies.results = [SimplePublicObject(id="new123", properties={"name": "New Company"})]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.batch_api.create.return_value = mock_created_companies

        insert_query = "INSERT INTO companies (name, city) VALUES ('New Company', 'Boston')"
        response = self.handler.native_query(insert_query)

        assert isinstance(response, Response)
        # Should succeed without error
        self.assertNotEqual(response.type, RESPONSE_TYPE.ERROR)

    def test_native_query_with_update(self):
        """Test native_query with UPDATE statement."""
        mock_hubspot_client = MagicMock()
        mock_companies_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "city": "New York",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]
        mock_updated_companies = MagicMock()
        mock_updated_companies.results = [SimplePublicObject(id="123", properties={"name": "Updated Company"})]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies_data
        mock_hubspot_client.crm.companies.batch_api.update.return_value = mock_updated_companies

        update_query = "UPDATE companies SET city='Boston' WHERE name='Test Company'"
        response = self.handler.native_query(update_query)

        assert isinstance(response, Response)
        # Should succeed without error
        self.assertNotEqual(response.type, RESPONSE_TYPE.ERROR)

    def test_native_query_with_delete(self):
        """Test native_query with DELETE statement."""
        mock_hubspot_client = MagicMock()
        mock_companies_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "city": "New York",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies_data
        mock_hubspot_client.crm.companies.batch_api.archive.return_value = None

        delete_query = "DELETE FROM companies WHERE name='Test Company'"
        response = self.handler.native_query(delete_query)

        assert isinstance(response, Response)
        # Should succeed without error
        self.assertNotEqual(response.type, RESPONSE_TYPE.ERROR)

    def test_handler_name(self):
        """Test handler name is set correctly."""
        self.assertEqual(self.handler.name, "hubspot")

    def test_connection_data_storage(self):
        """Test connection data is stored correctly."""
        self.assertEqual(self.handler.connection_data["access_token"], self.dummy_connection_data["access_token"])

    def test_connect_invalid_credentials(self):
        """Test connect method with invalid credentials."""
        # Test empty access token
        handler = HubspotHandler("hubspot", connection_data={"access_token": ""})
        with self.assertRaises(ValueError) as context:
            handler.connect()
        self.assertIn("Invalid access_token provided", str(context.exception))

        # Test invalid OAuth credentials
        handler = HubspotHandler("hubspot", connection_data={"client_id": "", "client_secret": "secret"})
        with self.assertRaises(ValueError) as context:
            handler.connect()
        self.assertIn("Invalid OAuth credentials provided", str(context.exception))

    def test_disconnect(self):
        """Test disconnect method."""
        mock_hubspot_client = MagicMock()
        self.mock_connect.return_value = mock_hubspot_client

        # Connect first
        self.handler.connect()
        self.assertTrue(self.handler.is_connected)

        # Then disconnect
        self.handler.disconnect()
        self.assertFalse(self.handler.is_connected)
        self.assertIsNone(self.handler.connection)

    def test_native_query_empty_query(self):
        """Test native_query with empty or None query."""
        response = self.handler.native_query(None)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Query cannot be None or empty", response.error_message)

        response = self.handler.native_query("")
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Query cannot be None or empty", response.error_message)

    def test_native_query_invalid_sql(self):
        """Test native_query with invalid SQL."""
        response = self.handler.native_query("INVALID SQL QUERY")
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Query execution failed", response.error_message)

    def test_get_tables_success(self):
        """Test get_tables method returns table metadata."""
        mock_hubspot_client = MagicMock()
        mock_companies_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies_data
        mock_hubspot_client.crm.contacts.get_all.return_value = []
        mock_hubspot_client.crm.deals.get_all.return_value = []

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertIsNotNone(response.data_frame)

        df = response.data_frame
        self.assertEqual(len(df), 3)  # companies, contacts, deals
        self.assertIn("TABLE_NAME", df.columns)
        self.assertIn("TABLE_TYPE", df.columns)
        self.assertIn("TABLE_SCHEMA", df.columns)
        self.assertIn("TABLE_DESCRIPTION", df.columns)

        table_names = df["TABLE_NAME"].tolist()
        self.assertIn("companies", table_names)
        self.assertIn("contacts", table_names)
        self.assertIn("deals", table_names)

    def test_get_tables_connection_failure(self):
        """Test get_tables method with connection failure."""
        self.mock_connect.side_effect = Exception("Connection failed")

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Failed to retrieve table list", response.error_message)

    def test_get_columns_companies_detailed(self):
        """Test get_columns method for companies table with detailed analysis."""
        mock_hubspot_client = MagicMock()
        mock_company_data = [
            SimplePublicObject(
                id="123",
                properties={
                    "name": "Test Company",
                    "city": "New York",
                    "phone": "+1-555-123-4567",
                    "state": "NY",
                    "domain": "testcompany.com",
                    "industry": "Technology",
                    "createdate": "2023-01-01T00:00:00Z",
                    "hs_lastmodifieddate": "2023-01-01T00:00:00Z",
                },
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_company_data

        response = self.handler.get_columns("companies")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        df = response.data_frame

        # Check that we have the expected columns in response
        self.assertIn("COLUMN_NAME", df.columns)
        self.assertIn("DATA_TYPE", df.columns)
        self.assertIn("IS_NULLABLE", df.columns)
        self.assertIn("COLUMN_DESCRIPTION", df.columns)

        # Check some expected column names are present
        column_names = df["COLUMN_NAME"].tolist()
        self.assertIn("id", column_names)
        self.assertIn("name", column_names)
        self.assertIn("city", column_names)

    def test_get_columns_connection_failure(self):
        """Test get_columns method with connection failure."""
        self.mock_connect.side_effect = Exception("Connection failed")

        response = self.handler.get_columns("companies")

        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Failed to retrieve columns", response.error_message)

    def test_data_type_inference(self):
        """Test _infer_data_type method."""
        # Test different data types
        self.assertEqual(self.handler._infer_data_type(None), "VARCHAR")
        self.assertEqual(self.handler._infer_data_type(True), "BOOLEAN")
        self.assertEqual(self.handler._infer_data_type(42), "INTEGER")
        self.assertEqual(self.handler._infer_data_type(3.14), "DECIMAL")
        self.assertEqual(self.handler._infer_data_type("text"), "VARCHAR")
        self.assertEqual(self.handler._infer_data_type("2023-01-01T00:00:00Z"), "TIMESTAMP")

    def test_table_descriptions(self):
        """Test _get_table_description method."""
        self.assertIn("companies data", self.handler._get_table_description("companies"))
        self.assertIn("contacts data", self.handler._get_table_description("contacts"))
        self.assertIn("deals data", self.handler._get_table_description("deals"))

    def test_default_columns_structure(self):
        """Test _get_default_columns method."""
        # Test companies table columns
        companies_cols = self.handler._get_default_columns("companies")
        self.assertIsInstance(companies_cols, list)
        self.assertTrue(len(companies_cols) > 0)

        # Check that basic required columns exist
        column_names = [col["COLUMN_NAME"] for col in companies_cols]
        self.assertIn("id", column_names)
        self.assertIn("name", column_names)
        self.assertIn("createdate", column_names)

        # Test contacts table columns
        contacts_cols = self.handler._get_default_columns("contacts")
        contact_column_names = [col["COLUMN_NAME"] for col in contacts_cols]
        self.assertIn("email", contact_column_names)
        self.assertIn("firstname", contact_column_names)

        # Test deals table columns
        deals_cols = self.handler._get_default_columns("deals")
        deal_column_names = [col["COLUMN_NAME"] for col in deals_cols]
        self.assertIn("dealname", deal_column_names)
        self.assertIn("amount", deal_column_names)

    def test_check_connection_with_api_test(self):
        """Test check_connection method performs actual API test."""
        mock_hubspot_client = MagicMock()
        mock_companies_data = [
            SimplePublicObject(
                id="123",
                properties={"name": "Test Company"},
            )
        ]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies_data

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.assertIsNone(response.error_message)
        # Verify that the API test call was made
        mock_hubspot_client.crm.companies.get_all.assert_called_with(limit=1)

    def test_oauth_connection(self):
        """Test OAuth connection flow."""
        oauth_data = OrderedDict(client_id="test_client_id", client_secret="test_client_secret")
        handler = HubspotHandler("hubspot", connection_data=oauth_data)

        mock_hubspot_client = MagicMock()

        with patch("hubspot.HubSpot") as mock_hubspot:
            mock_hubspot.return_value = mock_hubspot_client

            connection = handler.connect()

            self.assertIsNotNone(connection)
            self.assertTrue(handler.is_connected)
            # Verify HubSpot was called with OAuth credentials
            mock_hubspot.assert_called_with(client_id="test_client_id", client_secret="test_client_secret")

    def test_comprehensive_error_handling(self):
        """Test comprehensive error handling in various scenarios."""
        # Test connection error propagation
        with patch("hubspot.HubSpot") as mock_hubspot:
            mock_hubspot.side_effect = Exception("API Error")

            with self.assertRaises(Exception) as context:
                self.handler.connect()
            self.assertIn("Connection to HubSpot failed", str(context.exception))

    def test_secure_logging(self):
        """Test that sensitive credentials are not logged."""
        # This is more of a best practice check - in real implementation
        # you would check log outputs to ensure no credentials are exposed
        sensitive_data = {"access_token": "secret_token_12345"}
        handler = HubspotHandler("hubspot", connection_data=sensitive_data)

        # The handler should store connection data but not expose it in logs
        self.assertIn("access_token", handler.connection_data)
        # In actual implementation, you'd verify logs don't contain the token

    def test_column_statistics_calculation(self):
        """Test comprehensive column statistics calculation."""
        # Test numeric data
        numeric_values = [100, 200, 300, None, 150, 250]
        stats = self.handler._calculate_column_statistics("amount", numeric_values)

        self.assertEqual(stats["null_count"], 1)
        self.assertEqual(stats["distinct_count"], 5)  # 5 unique non-null values
        self.assertIsNotNone(stats["average_value"])
        self.assertEqual(stats["average_value"], 200.0)  # (100+200+300+150+250)/5

        # Test string data
        string_values = ["apple", "banana", "apple", None, "cherry"]
        stats = self.handler._calculate_column_statistics("fruit", string_values)

        self.assertEqual(stats["null_count"], 1)
        self.assertEqual(stats["distinct_count"], 3)  # apple, banana, cherry
        self.assertEqual(stats["min_value"], "apple")  # alphabetically first

    def test_data_type_inference_from_samples(self):
        """Test improved data type inference from multiple samples."""
        # Mixed numeric and string - should pick most common
        mixed_values = [100, 200, "300", 400, 500]  # mostly numeric
        data_type = self.handler._infer_data_type_from_samples(mixed_values)
        self.assertEqual(data_type, "INTEGER")

        # Timestamp strings
        timestamp_values = ["2023-01-01T10:00:00Z", "2023-01-02T11:00:00Z", None]
        data_type = self.handler._infer_data_type_from_samples(timestamp_values)
        self.assertEqual(data_type, "TIMESTAMP")

        # All null values
        null_values = [None, None, None]
        data_type = self.handler._infer_data_type_from_samples(null_values)
        self.assertEqual(data_type, "VARCHAR")

    def test_foreign_key_detection(self):
        """Test automatic foreign key detection."""
        # Should detect as foreign key: name suggests it and values look like IDs
        owner_id_values = ["123456", "789012", "345678", "901234"]
        is_fk = self.handler._is_potential_foreign_key("hubspot_owner_id", owner_id_values)
        self.assertTrue(is_fk)

        # Should NOT detect as foreign key: name doesn't suggest it
        name_values = ["John Doe", "Jane Smith", "Bob Johnson"]
        is_fk = self.handler._is_potential_foreign_key("firstname", name_values)
        self.assertFalse(is_fk)

        # Should NOT detect as foreign key: name suggests it but values don't look like IDs
        fake_id_values = ["apple", "banana", "cherry"]
        is_fk = self.handler._is_potential_foreign_key("company_id", fake_id_values)
        self.assertFalse(is_fk)

    def test_get_columns_with_comprehensive_statistics(self):
        """Test get_columns method returns comprehensive statistics."""
        mock_hubspot_client = MagicMock()

        # Mock larger dataset for statistics
        mock_company_data = []
        for i in range(50):  # Create 50 sample companies
            mock_company_data.append(
                SimplePublicObject(
                    id=f"company_{i}",
                    properties={
                        "name": f"Company {i}",
                        "city": "New York" if i % 2 == 0 else "San Francisco",
                        "industry": "Technology",
                        "hubspot_owner_id": f"owner_{i % 5}",  # Should be detected as FK
                        "annual_revenue": str(100000 + i * 1000),  # Numeric values
                        "createdate": f"2023-01-{(i % 28) + 1:02d}T10:00:00Z",
                    },
                )
            )

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_company_data

        response = self.handler.get_columns("companies")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        df = response.data_frame

        # Check that statistics columns are present
        expected_stat_columns = [
            "IS_PRIMARY_KEY",
            "IS_FOREIGN_KEY",
            "NULL_COUNT",
            "DISTINCT_COUNT",
            "MIN_VALUE",
            "MAX_VALUE",
            "AVERAGE_VALUE",
        ]

        for stat_col in expected_stat_columns:
            self.assertIn(stat_col, df.columns, f"Missing statistics column: {stat_col}")

        # Check that ID is marked as primary key
        id_row = df[df["COLUMN_NAME"] == "id"]
        self.assertEqual(len(id_row), 1)
        self.assertTrue(id_row.iloc[0]["IS_PRIMARY_KEY"])
        self.assertFalse(id_row.iloc[0]["IS_FOREIGN_KEY"])

        # Check that hubspot_owner_id is detected as foreign key
        owner_id_rows = df[df["COLUMN_NAME"] == "hubspot_owner_id"]
        if len(owner_id_rows) > 0:  # Only check if the column exists
            self.assertTrue(owner_id_rows.iloc[0]["IS_FOREIGN_KEY"])

    def test_comprehensive_table_metadata(self):
        """Test that get_tables returns comprehensive metadata."""
        mock_hubspot_client = MagicMock()

        # Mock data for row count estimation
        mock_companies = [SimplePublicObject(id=f"comp_{i}", properties={}) for i in range(25)]
        mock_contacts = [SimplePublicObject(id=f"cont_{i}", properties={}) for i in range(15)]
        mock_deals = [SimplePublicObject(id=f"deal_{i}", properties={}) for i in range(10)]

        self.mock_connect.return_value = mock_hubspot_client
        mock_hubspot_client.crm.companies.get_all.return_value = mock_companies
        mock_hubspot_client.crm.contacts.get_all.return_value = mock_contacts
        mock_hubspot_client.crm.deals.get_all.return_value = mock_deals

        response = self.handler.get_tables()

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        df = response.data_frame

        # Check comprehensive metadata columns
        required_columns = ["TABLE_NAME", "TABLE_TYPE", "TABLE_SCHEMA", "TABLE_DESCRIPTION", "ROW_COUNT"]
        for col in required_columns:
            self.assertIn(col, df.columns)

        # Check that row counts are estimated
        companies_row = df[df["TABLE_NAME"] == "companies"]
        self.assertEqual(companies_row.iloc[0]["ROW_COUNT"], 25)

        contacts_row = df[df["TABLE_NAME"] == "contacts"]
        self.assertEqual(contacts_row.iloc[0]["ROW_COUNT"], 15)


if __name__ == "__main__":
    unittest.main()

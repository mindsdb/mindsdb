from collections import OrderedDict
import pytest
import unittest
from unittest.mock import patch, MagicMock

try:
    from hubspot.crm.objects import SimplePublicObject
    from mindsdb.integrations.handlers.hubspot_handler.hubspot_handler import HubspotHandler
except ImportError:
    pytestmark = pytest.mark.skip("HubSpot handler not installed")

from base_handler_test import BaseHandlerTestSetup, BaseAPIHandlerTest

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)


class TestHubspotHandler(BaseHandlerTestSetup, BaseAPIHandlerTest, unittest.TestCase):
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
        return HubspotHandler("hubspot", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("hubspot.HubSpot")

    def test_initialization(self):
        """Test if the handler initializes correctly with proper values."""
        self.assertEqual(self.handler.name, "hubspot")
        self.assertFalse(self.handler.is_connected)
        self.assertEqual(self.handler.connection_data, self.dummy_connection_data)

        # Test that tables are registered
        self.assertIn("companies", self.handler._get_table_names())
        self.assertIn("contacts", self.handler._get_table_names())
        self.assertIn("deals", self.handler._get_table_names())

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


if __name__ == "__main__":
    unittest.main()

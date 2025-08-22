from collections import OrderedDict
import pytest
import unittest
from unittest.mock import patch, MagicMock

try:
    from salesforce_api.exceptions import AuthenticationError
    from mindsdb.integrations.handlers.salesforce_handler.salesforce_handler import SalesforceHandler
    from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import create_table_class
except ImportError:
    pytestmark = pytest.mark.skip("Salesforce handler not installed")

from mindsdb_sql_parser.ast import BinaryOperation, Constant, Identifier, Select, Star
from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup

from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)


class TestSalesforceHandler(BaseHandlerTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
        )

    def create_handler(self):
        return SalesforceHandler("salesforce", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("salesforce_api.Salesforce")

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        Also, verifies that salesforce_api.Salesforce is instantiated exactly once.
        """
        self.mock_connect.return_value = MagicMock()
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once()

    def test_check_connection_success(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

    def test_check_connection_failure(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a failed connection.
        """
        self.mock_connect.side_effect = AuthenticationError("Invalid credentials")
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

    def test_get_tables(self):
        """
        Test that the `get_tables` method returns a list of tables mapped from the Salesforce API.
        """
        mock_tables = ["Account", "Contact"]
        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(
                describe=lambda: {"sobjects": [{"name": table, "queryable": True} for table in mock_tables]}
            )
        )
        self.handler.connect()
        response = self.handler.get_tables()

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_tables))
        self.assertEqual(list(df["table_name"]), [name.lower() for name in mock_tables])

    def test_get_columns(self):
        """
        Test that the `get_columns` method returns a list of columns for a given table.
        """
        mock_columns = ["Id", "Name", "Email"]
        mock_table = "Contact"

        # Create a mock for the Contact object that will be accessed via getattr
        contact_mock = MagicMock()
        contact_mock.describe.return_value = {"fields": [{"name": column} for column in mock_columns]}

        # Create the main sobjects mock
        sobjects_mock = MagicMock()
        sobjects_mock.describe.return_value = {"sobjects": [{"name": mock_table, "queryable": True}]}

        # Set the contact attribute (lowercase) on the sobjects mock since the handler uses resource_name.lower()
        setattr(sobjects_mock, mock_table.lower(), contact_mock)

        # Create the client mock
        client_mock = MagicMock()
        client_mock.sobjects = sobjects_mock

        # Make sure the mock_connect always returns the same client mock
        self.mock_connect.return_value = client_mock

        self.handler.connect()
        response = self.handler.get_columns(mock_table)

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)

        df = response.data_frame
        self.assertEqual(len(df), len(mock_columns))
        self.assertEqual(list(df["Field"]), mock_columns)

    def test_pre_filtering_with_include_tables(self):
        """
        Test that pre-filtering works correctly when include_tables is specified.
        """
        # Setup handler with include_tables
        connection_data = OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
            include_tables=["Account", "Contact"],
        )
        handler = SalesforceHandler("salesforce", connection_data=connection_data)

        # Mock connection and individual table describe calls
        mock_connection = MagicMock()
        mock_connection.sobjects.Account = MagicMock()
        mock_connection.sobjects.Account.describe.return_value = {"queryable": True}
        mock_connection.sobjects.Contact = MagicMock()
        mock_connection.sobjects.Contact.describe.return_value = {"queryable": True}

        # Set the connection attribute directly and mark as connected
        handler.connection = mock_connection
        handler.is_connected = True

        resource_names = handler._get_resource_names()

        # Should only return the specified tables
        self.assertEqual(set(resource_names), {"Account", "Contact"})

        # Should NOT call global describe() method
        mock_connection.sobjects.describe.assert_not_called()

        # Should call individual table describe() methods
        mock_connection.sobjects.Account.describe.assert_called_once()
        mock_connection.sobjects.Contact.describe.assert_called_once()

    def test_pre_filtering_with_tables_parameter(self):
        """
        Test that pre-filtering works correctly when 'tables' parameter is specified (alternative to include_tables).
        """
        # Setup handler with 'tables' parameter
        connection_data = OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
            tables=["Lead", "Opportunity"],
        )
        handler = SalesforceHandler("salesforce", connection_data=connection_data)

        # Mock connection and individual table describe calls
        mock_connection = MagicMock()
        mock_connection.sobjects.Lead = MagicMock()
        mock_connection.sobjects.Lead.describe.return_value = {"queryable": True}
        mock_connection.sobjects.Opportunity = MagicMock()
        mock_connection.sobjects.Opportunity.describe.return_value = {"queryable": True}

        # Set the connection attribute directly and mark as connected
        handler.connection = mock_connection
        handler.is_connected = True

        resource_names = handler._get_resource_names()

        # Should only return the specified tables
        self.assertEqual(set(resource_names), {"Lead", "Opportunity"})

        # Should NOT call global describe() method
        mock_connection.sobjects.describe.assert_not_called()

        # Should call individual table describe() methods
        mock_connection.sobjects.Lead.describe.assert_called_once()
        mock_connection.sobjects.Opportunity.describe.assert_called_once()

    def test_pre_filtering_with_exclude_tables(self):
        """
        Test that exclude_tables parameter properly filters out specified tables.
        """
        # Setup handler with include_tables and exclude_tables
        connection_data = OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
            include_tables=["Account", "Contact", "Lead"],
            exclude_tables=["Lead"],
        )
        handler = SalesforceHandler("salesforce", connection_data=connection_data)

        # Mock connection and individual table describe calls
        mock_connection = MagicMock()
        mock_connection.sobjects.Account = MagicMock()
        mock_connection.sobjects.Account.describe.return_value = {"queryable": True}
        mock_connection.sobjects.Contact = MagicMock()
        mock_connection.sobjects.Contact.describe.return_value = {"queryable": True}
        mock_connection.sobjects.Lead = MagicMock()
        mock_connection.sobjects.Lead.describe.return_value = {"queryable": True}

        # Set the connection attribute directly and mark as connected
        handler.connection = mock_connection
        handler.is_connected = True

        resource_names = handler._get_resource_names()

        # Should only return Account and Contact (Lead should be excluded)
        self.assertEqual(set(resource_names), {"Account", "Contact"})

        # Should NOT call global describe() method
        mock_connection.sobjects.describe.assert_not_called()

        # Should call describe() only for non-excluded tables
        mock_connection.sobjects.Account.describe.assert_called_once()
        mock_connection.sobjects.Contact.describe.assert_called_once()
        mock_connection.sobjects.Lead.describe.assert_not_called()

    def test_fallback_to_full_discovery_when_no_include_tables(self):
        """
        Test that fallback to full discovery works when no include_tables/tables specified.
        """
        # Setup handler without include_tables or tables
        connection_data = OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
        )
        handler = SalesforceHandler("salesforce", connection_data=connection_data)

        # Mock connection with global describe response
        mock_connection = MagicMock()
        mock_connection.sobjects.describe.return_value = {
            "sobjects": [
                {"name": "Account", "queryable": True},
                {"name": "Contact", "queryable": True},
                {"name": "AccountHistory", "queryable": True},  # Should be filtered out by hard-coded rules
                {"name": "CustomObject__c", "queryable": True},
            ]
        }

        # Set the connection attribute directly and mark as connected
        handler.connection = mock_connection
        handler.is_connected = True

        resource_names = handler._get_resource_names()

        # Should return filtered tables (excluding History tables per hard-coded rules)
        self.assertIn("Account", resource_names)
        self.assertIn("Contact", resource_names)
        self.assertIn("CustomObject__c", resource_names)
        self.assertNotIn("AccountHistory", resource_names)  # Should be filtered out

        # Should call global describe() method for full discovery
        mock_connection.sobjects.describe.assert_called_once()

    def test_error_handling_for_non_existent_tables(self):
        """
        Test that non-existent tables are handled gracefully with warnings.
        """
        # Setup handler with include_tables containing non-existent table
        connection_data = OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
            include_tables=["Account", "NonExistentTable", "Contact"],
        )
        handler = SalesforceHandler("salesforce", connection_data=connection_data)

        # Mock connection where NonExistentTable raises an exception
        mock_connection = MagicMock()
        mock_connection.sobjects.Account = MagicMock()
        mock_connection.sobjects.Account.describe.return_value = {"queryable": True}
        mock_connection.sobjects.Contact = MagicMock()
        mock_connection.sobjects.Contact.describe.return_value = {"queryable": True}

        # Mock NonExistentTable to raise an exception
        mock_non_existent = MagicMock()
        mock_non_existent.describe.side_effect = Exception("Table not found")
        mock_connection.sobjects.NonExistentTable = mock_non_existent

        # Set the connection attribute directly and mark as connected
        handler.connection = mock_connection
        handler.is_connected = True

        resource_names = handler._get_resource_names()

        # Should only return existing tables (Account, Contact) and skip non-existent one
        self.assertEqual(set(resource_names), {"Account", "Contact"})

        # Should NOT call global describe() method
        mock_connection.sobjects.describe.assert_not_called()

        # Should call describe() for all specified tables (including non-existent)
        mock_connection.sobjects.Account.describe.assert_called_once()
        mock_connection.sobjects.Contact.describe.assert_called_once()
        mock_connection.sobjects.NonExistentTable.describe.assert_called_once()


class TestSalesforceAnyTable(BaseAPIResourceTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            username="demo@example.com",
            password="demo_password",
            client_id="3MVG9lKcPoNINVBIPJjdw1J9LLM82HnZz9Yh7ZJnY",
            client_secret="5A52C1A1E21DF9012IODC9ISNXXAADDA9",
        )

    def create_handler(self):
        return SalesforceHandler("salesforce", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("salesforce_api.Salesforce")

    def create_resource(self):
        return create_table_class(self.table_name)(self.handler)

    def setUp(self):
        """
        Set up common test fixtures.
        """
        self.table_name = "Contact"
        self.mock_columns = ["Id", "Name", "Email"]
        self.mock_record = {column: f"{column}_value" for column in self.mock_columns}

        super().setUp()

        self.mock_connect.return_value = MagicMock(
            sobjects=MagicMock(query=lambda query: [{"attributes": {"type": self.table_name}, **self.mock_record}]),
            Contact=MagicMock(describe=lambda: {"fields": [{"name": column} for column in self.mock_columns]}),
        )

    def test_select_all(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a simple SELECT * query.
        """
        select_query = Select(targets=[Star()], from_table=Identifier(parts=[self.table_name]))
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns.
        """
        select_query = Select(
            targets=[Identifier(parts=[column]) for column in self.mock_columns],
            from_table=Identifier(parts=[self.table_name]),
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_alias(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and aliases.
        """
        select_query = Select(
            targets=[
                Identifier(parts=[column], alias=Identifier(parts=[f"{column}_alias"])) for column in self.mock_columns
            ],
            from_table=Identifier(parts=[self.table_name]),
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), [f"{column}_alias" for column in self.mock_columns])
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_condition(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and a WHERE condition.
        """
        select_query = Select(
            targets=[Identifier(parts=[column]) for column in self.mock_columns],
            from_table=Identifier(parts=[self.table_name]),
            where=BinaryOperation(op="=", args=[Identifier("Id"), Constant("Id_value")]),
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_condition_and_limit(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns, a WHERE condition, and a LIMIT clause.
        """
        select_query = Select(
            targets=[Identifier(parts=[column]) for column in self.mock_columns],
            from_table=Identifier(parts=[self.table_name]),
            where=BinaryOperation(op="=", args=[Identifier("Id"), Constant("Id_value")]),
            limit=Constant(1),
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    def test_select_columns_with_conditions(self):
        """
        Test that the `select` method returns the data from the Salesforce resource for a SELECT query with specific columns and multiple WHERE conditions.
        """
        select_query = Select(
            targets=[Identifier(parts=[column]) for column in self.mock_columns],
            from_table=Identifier(parts=[self.table_name]),
            where=BinaryOperation(
                op="AND",
                args=[
                    BinaryOperation(op="=", args=[Identifier("Id"), Constant("Id_value")]),
                    BinaryOperation(op="=", args=[Identifier("Name"), Constant("Name_value")]),
                ],
            ),
        )
        df = self.resource.select(select_query)

        self.assertEqual(len(df), 1)
        self.assertEqual(list(df.columns), self.mock_columns)
        self.assertEqual(list(df.iloc[0]), list(self.mock_record.values()))

    # TODO: Add tests for `add`, `modify`, and `remove` methods.


if __name__ == "__main__":
    unittest.main()

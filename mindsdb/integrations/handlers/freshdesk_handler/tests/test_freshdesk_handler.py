import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.freshdesk_handler.freshdesk_handler import FreshdeskHandler
from mindsdb.integrations.handlers.freshdesk_handler.freshdesk_tables import (
    FreshdeskAgentsTable,
    FreshdeskTicketsTable,
)


class TestFreshdeskHandler(unittest.TestCase):
    """Test cases for Freshdesk Handler"""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures before running tests."""
        cls.kwargs = {
            "connection_data": {
                "domain": "test.freshdesk.com",
                "api_key": "test_api_key_123"
            }
        }
        cls.handler = FreshdeskHandler("test_freshdesk_handler", **cls.kwargs)
        cls.agents_table = FreshdeskAgentsTable(cls.handler)
        cls.tickets_table = FreshdeskTicketsTable(cls.handler)

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Mock the freshdesk client
        self.mock_client = Mock()
        self.handler.freshdesk_client = self.mock_client
        self.handler.is_connected = True

    def _get_agents_mock_data(self, num_records=3):
        """Helper method to create mock agents data."""
        return pd.DataFrame({
            'id': list(range(1, num_records + 1)),
            'available': ([True, False, True] * ((num_records // 3) + 1))[:num_records],
            'contact_email': [f'agent{i}@test.com' for i in range(1, num_records + 1)],
            'contact_mobile': [f'123456789{i}' for i in range(1, num_records + 1)],
            'contact_name': [f'Agent {i}' for i in range(1, num_records + 1)]
        })

    def _get_tickets_mock_data(self, num_records=3, custom_data=None):
        """Helper method to create mock tickets data."""
        base_data = {
            "id": list(range(1, num_records + 1)),
            "status": list(range(2, num_records + 2)),
            "priority": list(range(1, num_records + 1)),
            "subject": [f"Issue {i}" for i in range(1, num_records + 1)],
            "group_id": list(range(1, num_records + 1))
        }

        # Override with custom data if provided
        if custom_data:
            base_data.update(custom_data)

        return pd.DataFrame(base_data)

    def test_agents_table_get_conditions(self):
        """Test get_conditions method for agents table."""
        where_conditions = [
            ['=', 'contact_email', 'test@example.com'],
            ['>', 'id', 100],
            ['=', 'contact_mobile', '+1234567890']
        ]

        subset_conditions, filter_conditions = self.agents_table.get_conditions(
            where_conditions)

        # Check that API filter conditions are properly extracted
        expected_filter_conditions = {
            'email': 'test@example.com',
            'mobile': '+1234567890'
        }
        self.assertEqual(filter_conditions, expected_filter_conditions)

        # Check that non-API filter conditions are in subset
        expected_subset = [['>', 'id', 100]]
        self.assertEqual(subset_conditions, expected_subset)

    def test_agents_table_select_basic(self):
        """Test basic select query for agents table."""
        mock_df = self._get_agents_mock_data()

        with patch.object(self.agents_table, 'get_freshdesk_agents', return_value=mock_df):
            query = "SELECT id, available FROM agents"
            ast = parse_sql(query)
            result = self.agents_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
            self.assertIn('id', result.columns)
            self.assertIn('available', result.columns)

    def test_agents_table_select_with_where(self):
        """Test select query with WHERE clause for agents table."""
        mock_df = self._get_agents_mock_data()

        with patch.object(self.agents_table, 'get_freshdesk_agents', return_value=mock_df):
            query = "SELECT id, available, contact_email FROM agents WHERE contact_email = 'agent1@test.com'"
            ast = parse_sql(query)
            result = self.agents_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)

    def test_tickets_table_get_conditions(self):
        """Test get_conditions method for tickets table."""
        where_conditions = [
            ['=', 'status', 'open'],
            ['=', 'priority', 'high'],
            ['>', 'id', 100],
            ['=', 'group_id', 5]
        ]

        subset_conditions, search_conditions = self.tickets_table.get_conditions(
            where_conditions)

        # Check that API filter conditions are properly extracted
        expected_search_conditions = [
            ('=', 'status', 2),
            ('=', 'priority', 3),
            ('=', 'group_id', 5)
        ]
        self.assertEqual(search_conditions, expected_search_conditions)

        # Check that non-API filter conditions are in subset
        expected_subset = [['>', 'id', 100]]
        self.assertEqual(subset_conditions, expected_subset)

    def test_tickets_table_priority_status_mapping(self):
        """Test priority and status mapping in get_conditions."""
        where_conditions = [
            ['=', 'priority', 'urgent'],
            ['=', 'status', 'closed']
        ]

        subset_conditions, search_conditions = self.tickets_table.get_conditions(
            where_conditions)

        # Check that string values are mapped to numbers
        expected_search_conditions = [
            ('=', 'priority', 4),
            ('=', 'status', 5)
        ]
        self.assertEqual(search_conditions, expected_search_conditions)

    def test_tickets_table_build_freshdesk_api_filter_query(self):
        """Test build_freshdesk_api_filter_query method."""
        conditions = [
            ('=', 'status', 2),
            ('=', 'priority', 3)
        ]

        result = self.tickets_table.build_freshdesk_api_filter_query(
            conditions)

        # Should return a URL-encoded query string
        self.assertIn('status%3A2', result)
        self.assertIn('priority%3A3', result)
        self.assertIn('AND', result)

    def test_tickets_table_build_freshdesk_api_filter_query_with_strings(self):
        """Test build_freshdesk_api_filter_query with string values."""
        conditions = [
            ('=', 'status', 'open'),
            ('=', 'priority', 'high')
        ]

        result = self.tickets_table.build_freshdesk_api_filter_query(
            conditions)

        # Should handle string values with quotes
        self.assertIn("status%3A%27open%27", result)
        self.assertIn("priority%3A%27high%27", result)

    def test_tickets_table_select_basic(self):
        """Test basic select query for tickets table."""
        mock_df = self._get_tickets_mock_data()

        with patch.object(self.tickets_table, 'get_freshdesk_tickets', return_value=mock_df):
            query = "SELECT id, status, subject FROM tickets"
            ast = parse_sql(query)
            result = self.tickets_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)
            self.assertIn('id', result.columns)
            self.assertIn('status', result.columns)
            self.assertIn('subject', result.columns)

    def test_tickets_table_select_with_where(self):
        """Test select query with WHERE clause for tickets table."""
        mock_df = self._get_tickets_mock_data()

        with patch.object(self.tickets_table, 'get_freshdesk_tickets', return_value=mock_df):
            query = "SELECT id, status, subject FROM tickets WHERE status = 'open'"
            ast = parse_sql(query)
            result = self.tickets_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)

    def test_tickets_table_select_with_limit(self):
        """Test select query with LIMIT clause for tickets table."""
        mock_df = self._get_tickets_mock_data(num_records=5)

        with patch.object(self.tickets_table, 'get_freshdesk_tickets', return_value=mock_df):
            query = "SELECT id, status, subject FROM tickets LIMIT 3"
            ast = parse_sql(query)
            result = self.tickets_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertLessEqual(len(result), 3)

    def test_tickets_table_select_with_order_by(self):
        """Test select query with ORDER BY clause for tickets table."""
        # Create custom data with different order for testing
        custom_data = {
            "id": [3, 1, 2],
            "status": [4, 2, 3],
            "priority": [3, 1, 2],
            "subject": ["Issue 3", "Issue 1", "Issue 2"],
            "group_id": [3, 1, 2]
        }
        mock_df = self._get_tickets_mock_data(custom_data=custom_data)

        with patch.object(self.tickets_table, 'get_freshdesk_tickets', return_value=mock_df):
            query = "SELECT id, status, subject FROM tickets ORDER BY id"
            ast = parse_sql(query)
            result = self.tickets_table.select(ast)

            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()

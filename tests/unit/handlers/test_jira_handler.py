"""Unit tests for Jira handler."""

from collections import OrderedDict
import pytest
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

try:
    from mindsdb.integrations.handlers.community.jira_handler.jira_handler import JiraHandler
    from mindsdb.integrations.handlers.community.jira_handler.jira_tables import (
        JiraIssuesTable,
        JiraUsersTable,
        JiraProjectsTable,
        JiraGroupsTable,
    )
except ImportError:
    pytestmark = pytest.mark.skip("Jira handler not installed")

from base_handler_test import BaseAPIHandlerTest


class TestJiraHandler(BaseAPIHandlerTest, unittest.TestCase):
    """Test Jira handler following standard test patterns."""

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            url="https://test.atlassian.net",
            username="test@example.com",
            api_token="test_token_12345",
        )

    @property
    def registered_tables(self):
        return ["projects", "issues", "groups", "users"]

    @property
    def err_to_raise_on_connect_failure(self):
        return Exception("Authentication failed")

    def create_handler(self):
        return JiraHandler("test_jira", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.community.jira_handler.jira_handler.Jira")

    def test_issues_table_missing_assignee(self):
        """Test that issues without assignee are handled correctly."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        mock_issues = [
            {
                "id": "1",
                "key": "TEST-1",
                "fields": {
                    "project": {"id": "10001", "key": "TEST", "name": "Test Project"},
                    "summary": "Issue with assignee",
                    "priority": {"name": "High"},
                    "creator": {"displayName": "John Doe"},
                    "assignee": {"displayName": "Jane Smith"},
                    "status": {"name": "In Progress"},
                },
            },
            {
                "id": "2",
                "key": "TEST-2",
                "fields": {
                    "project": {"id": "10001", "key": "TEST", "name": "Test Project"},
                    "summary": "Unassigned issue",
                    "priority": {"name": "Medium"},
                    "creator": {"displayName": "John Doe"},
                    "status": {"name": "Open"},
                },
            },
            {
                "id": "3",
                "key": "TEST-3",
                "fields": {
                    "project": {"id": "10001", "key": "TEST", "name": "Test Project"},
                    "summary": "Issue without priority",
                    "creator": {"displayName": "John Doe"},
                    "status": {"name": "Done"},
                },
            },
        ]

        mock_client.get_all_projects.return_value = [{"id": "10001"}]
        mock_client.get_all_project_issues.return_value = mock_issues

        issues_table = JiraIssuesTable(self.handler)
        result_df = issues_table.list(conditions=[])

        self.assertEqual(len(result_df), 3)
        self.assertIsNotNone(result_df)

        expected_columns = issues_table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result_df.columns)

        self.assertEqual(result_df.loc[0, "assignee"], "Jane Smith")
        self.assertTrue(pd.isna(result_df.loc[1, "assignee"]))
        self.assertTrue(pd.isna(result_df.loc[2, "assignee"]))

        self.assertEqual(result_df.loc[0, "priority"], "High")
        self.assertEqual(result_df.loc[1, "priority"], "Medium")
        self.assertTrue(pd.isna(result_df.loc[2, "priority"]))

    def test_users_table_missing_timezone(self):
        """Test that users without timeZone field are handled correctly."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        mock_users = [
            {
                "accountId": "user1",
                "accountType": "atlassian",
                "emailAddress": "user1@example.com",
                "displayName": "User One",
                "active": True,
                "timeZone": "America/New_York",
                "locale": "en_US",
            },
            {
                "accountId": "user2",
                "accountType": "atlassian",
                "emailAddress": "user2@example.com",
                "displayName": "User Two",
                "active": True,
                "locale": "en_US",
            },
            {
                "accountId": "user3",
                "accountType": "atlassian",
                "displayName": "User Three",
                "active": False,
            },
        ]

        mock_client.users_get_all.return_value = mock_users

        users_table = JiraUsersTable(self.handler)
        result_df = users_table.list(conditions=[])

        self.assertEqual(len(result_df), 3)
        self.assertIsNotNone(result_df)

        expected_columns = users_table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result_df.columns)

        self.assertEqual(result_df.loc[0, "timeZone"], "America/New_York")
        self.assertTrue(pd.isna(result_df.loc[1, "timeZone"]))
        self.assertTrue(pd.isna(result_df.loc[2, "timeZone"]))

        self.assertEqual(result_df.loc[0, "emailAddress"], "user1@example.com")
        self.assertEqual(result_df.loc[1, "emailAddress"], "user2@example.com")
        self.assertTrue(pd.isna(result_df.loc[2, "emailAddress"]))

    def test_projects_table_missing_optional_fields(self):
        """Test that projects with missing optional fields are handled correctly."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        mock_projects = [
            {
                "id": "10001",
                "key": "PROJ1",
                "name": "Project One",
                "projectTypeKey": "software",
                "simplified": True,
                "style": "classic",
                "isPrivate": False,
                "entityId": "entity1",
                "uuid": "uuid1",
            },
            {
                "id": "10002",
                "key": "PROJ2",
                "name": "Project Two",
            },
        ]

        mock_client.get_all_projects.return_value = mock_projects

        projects_table = JiraProjectsTable(self.handler)
        result_df = projects_table.list(conditions=[])

        self.assertEqual(len(result_df), 2)
        self.assertIsNotNone(result_df)

        expected_columns = projects_table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result_df.columns)

        self.assertEqual(result_df.loc[0, "projectTypeKey"], "software")
        self.assertTrue(pd.isna(result_df.loc[1, "projectTypeKey"]))

    def test_groups_table_missing_fields(self):
        """Test that groups with missing fields are handled correctly."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        mock_groups = {
            "groups": [
                {
                    "groupId": "group1",
                    "name": "Developers",
                    "html": "<a>Developers</a>",
                },
                {
                    "groupId": "group2",
                    "name": "Managers",
                },
            ]
        }

        mock_client.get_groups.return_value = mock_groups

        groups_table = JiraGroupsTable(self.handler)
        result_df = groups_table.list(conditions=[])

        self.assertEqual(len(result_df), 2)
        self.assertIsNotNone(result_df)

        expected_columns = groups_table.get_columns()
        for col in expected_columns:
            self.assertIn(col, result_df.columns)

        self.assertEqual(result_df.loc[0, "html"], "<a>Developers</a>")
        self.assertTrue(pd.isna(result_df.loc[1, "html"]))


if __name__ == "__main__":
    unittest.main()

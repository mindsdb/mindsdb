import pytest
import unittest

from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError

import pandas as pd


from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)

try:
    from mindsdb.integrations.handlers.jira_handler.jira_handler import JiraHandler
    from mindsdb.integrations.handlers.jira_handler.jira_tables import (
        JiraAttachmentsTable,
        JiraCommentsTable,
        JiraIssuesTable,
        JiraUsersTable,
        JiraProjectsTable,
        JiraGroupsTable,
        SERVER_COLUMNS,
    )
except ImportError:
    pytestmark = pytest.mark.skip("Jira handler not installed")


class TestJiraHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return {
            "jira_url": "https://your-domain.atlassian.net",
            "jira_username": "username",
            "jira_api_token": "your_api_token",
            "is_cloud": False,
        }

    @property
    def err_to_raise_on_connect_failure(self):
        return HTTPError("Failed to connect to Jira")

    def create_handler(self):
        return JiraHandler("jira", self.dummy_connection_data)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.jira_handler.jira_handler.Jira")

    def test_connect_cloud_success(self):
        """Ensure cloud connections normalize credentials and reuse Jira constructor correctly."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        connection = self.handler.connect()

        self.assertIs(connection, mock_client)
        self.assertTrue(self.handler.is_connected)
        self.mock_connect.assert_called_once_with(
            username=self.dummy_connection_data["jira_username"],
            password=self.dummy_connection_data["jira_api_token"],
            url=self.dummy_connection_data["jira_url"],
            cloud=True,
        )

    def test_connect_reuse_existing_connection(self):
        """If already connected, connect should reuse the existing client."""
        cached_connection = MagicMock()
        self.handler.connection = cached_connection
        self.handler.is_connected = True

        connection = self.handler.connect()

        self.assertIs(connection, cached_connection)
        self.mock_connect.assert_not_called()

    def test_connect_runtime_error_on_missing_cached_connection(self):
        """Marking the handler as connected without a cached client should raise."""
        self.handler.is_connected = True
        self.handler.connection = None

        with self.assertRaises(RuntimeError):
            self.handler.connect()

    def test_check_connection_http_error(self):
        """check_connection should surface HTTP errors from the Jira client."""
        mock_client = MagicMock()
        mock_client.myself.side_effect = HTTPError("Unauthorized")
        self.mock_connect.return_value = mock_client

        response = self.handler.check_connection()

        assert isinstance(response, StatusResponse)
        self.assertFalse(response.success)
        self.assertIn("Unauthorized", response.error_message)
        self.assertFalse(self.handler.is_connected)

    def test_native_query_http_error(self):
        """native_query should return an error response when Jira raises HTTPError."""
        mock_client = MagicMock()
        mock_client.jql.side_effect = HTTPError("Bad JQL")
        self.mock_connect.return_value = mock_client

        response = self.handler.native_query("project = TEST")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.ERROR)
        self.assertIn("Bad JQL", response.error_message)

    def test_native_query_returns_empty_dataframe_when_no_issues(self):
        """Ensure native_query returns an empty dataframe with expected columns."""
        mock_client = MagicMock()
        mock_client.jql.return_value = {}
        self.mock_connect.return_value = mock_client

        response = self.handler.native_query("project = TEST")

        assert isinstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertTrue(response.data_frame.empty)
        issues_columns = JiraIssuesTable(self.handler).get_columns()
        self.assertListEqual(list(response.data_frame.columns), issues_columns)

    def test_attachments_table_fetches_missing_fields(self):
        """Attachments table should refresh issues to retrieve missing attachment fields."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        issue_without_attachments = {"id": "1", "key": "ISSUE-1", "fields": {}}
        mock_client.get_all_projects.return_value = [{"id": "100"}]
        mock_client.get_all_project_issues.return_value = [issue_without_attachments]
        mock_client.get_issue.return_value = {
            "fields": {
                "attachment": [
                    {"id": "att-1", "filename": "log.txt", "size": 10, "mimeType": "text/plain"}
                ]
            }
        }

        attachments_table = JiraAttachmentsTable(self.handler)
        result_df = attachments_table.list(limit=1)

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.loc[0, "attachment_id"], "att-1")
        self.assertEqual(result_df.loc[0, "issue_key"], "ISSUE-1")
        self.assertEqual(result_df.loc[0, "filename"], "log.txt")

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

    def test_comments_table_fetches_missing_fields(self):
        """Comments table should refresh issues to retrieve missing comment fields."""
        mock_client = MagicMock()
        self.mock_connect.return_value = mock_client

        issue_without_comments = {"id": "1", "key": "ISSUE-1", "fields": {}}
        mock_client.get_all_projects.return_value = [{"id": "100"}]
        mock_client.get_all_project_issues.return_value = [issue_without_comments]
        mock_client.get_issue.return_value = {
            "fields": {
                "comment": {
                    "comments": [
                        {
                            "id": "c-1",
                            "body": "First comment",
                            "created": "2024-01-01",
                            "updated": "2024-01-02",
                            "author": {
                                "displayName": "Commenter",
                                "accountId": "acc-1",
                            },
                            "visibility": {
                                "type": "role",
                                "value": "admin",
                            },
                        }
                    ]
                }
            }
        }

        comments_table = JiraCommentsTable(self.handler)
        result_df = comments_table.list(limit=1)

        self.assertEqual(len(result_df), 1)
        self.assertEqual(result_df.loc[0, "comment_id"], "c-1")
        self.assertEqual(result_df.loc[0, "issue_key"], "ISSUE-1")
        self.assertEqual(result_df.loc[0, "body"], "First comment")
        self.assertEqual(result_df.loc[0, "author"], "Commenter")
        self.assertEqual(result_df.loc[0, "visibility_type"], "role")
        self.assertEqual(result_df.loc[0, "visibility_value"], "admin")

    def test_users_table_server_mode_columns(self):
        """Users table should switch to server columns when client.cloud is False."""
        mock_client = MagicMock()
        mock_client.cloud = False
        self.mock_connect.return_value = mock_client

        mock_client.user.return_value = {
            "name": "serveruser",
            "displayName": "Server User",
            "emailAddress": "server@example.com",
        }

        users_table = JiraUsersTable(self.handler)
        result_df = users_table.list()

        self.assertEqual(len(result_df), 1)
        self.assertListEqual(list(result_df.columns), SERVER_COLUMNS)
        self.assertEqual(result_df.loc[0, "name"], "serveruser")
        self.assertEqual(result_df.loc[0, "displayName"], "Server User")
        self.assertEqual(result_df.loc[0, "emailAddress"], "server@example.com")


if __name__ == "__main__":
    unittest.main()

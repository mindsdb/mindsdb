import unittest
from unittest.mock import patch, Mock

import pandas as pd

from mindsdb_sql.parser import ast
from mindsdb_sql.parser.ast import  Identifier

from mindsdb.integrations.handlers.jira_handler.jira_handler import JiraHandler
from mindsdb.integrations.handlers.jira_handler.jira_table import JiraProjectsTable

class TestJiraProjectsTable(unittest.TestCase):
    """
    Tests for the JiraProjectsTable class.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the tests.
        """
        cls.api_handler = Mock(spec=JiraHandler)
        cls.api_handler.connection_data = {'project': 'TEST_PROJECT'}

    def test_get_columns_returns_all_columns(self):
        """
        Test that get_columns returns all columns.
        """
        projects_table = JiraProjectsTable(self.api_handler)
        self.assertListEqual(projects_table.get_columns(), [
            'key',
            'summary',
            'status',
            'reporter',
            'assignee',
            'priority',
        ])

    @patch.object(JiraProjectsTable, 'call_jira_api')
    def test_select_star_for_all_projects_returns_all_columns(self, mock_call_jira_api):
        """
        Test that a SELECT * query returns all columns with correct data.
        """
        mock_call_jira_api.return_value = TEST_PROJECTS_DATA

        projects_table = JiraProjectsTable(self.api_handler)

        select_all = ast.Select(
        
            targets=[ast.Star()],
            from_table="projects",
        )

        all_projects = projects_table.select(select_all)
        first_project = all_projects.iloc[0]

        self.assertEqual(all_projects.shape[1], len(projects_table.get_columns()))
        self.assertEqual(first_project["key"], "TEST-1")
        self.assertEqual(first_project["summary"], "Test Issue 1")

    @patch.object(JiraProjectsTable, 'call_jira_api')
    def test_select_for_all_projects_returns_only_selected_columns(self, mock_call_jira_api):
        """
        Test that SELECT with specific columns returns the selected columns.
        """
        
        mock_call_jira_api.return_value = TEST_PROJECTS_DATA

        projects_table = JiraProjectsTable(self.api_handler)

        select_all = ast.Select(
            
            targets=[
                Identifier('key'),
                Identifier('summary'),
            ],
            from_table="projects",
        )

        all_projects = projects_table.select(select_all)
        first_project = all_projects.iloc[0]

        self.assertEqual(all_projects.shape[1], 2)
        self.assertEqual(first_project["key"], "TEST-1")
        self.assertEqual(first_project["summary"], "Test Issue 1")
    

# Test data
TEST_PROJECTS_DATA = pd.DataFrame([
    {
        'key': 'TEST-1',
        'summary': 'Test Issue 1',
        'status': 'Open',
        'reporter': 'John Doe',
        'assignee': 'Jane Smith',
        'priority': 'High'
    },
    {
        'key': 'TEST-2',
        'summary': 'Test Issue 2',
        'status': 'Closed',
        'reporter': 'Jane Smith',
        'assignee': 'John Doe',
        'priority': 'Low'
    }
])

if __name__ == "__main__":
    unittest.main()

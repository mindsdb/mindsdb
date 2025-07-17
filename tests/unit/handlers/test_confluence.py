from collections import OrderedDict
import unittest
from unittest.mock import patch, MagicMock

import pandas as pd

from base_handler_test import BaseHandlerTestSetup, BaseAPIResourceTestSetup
from mindsdb.integrations.handlers.confluence_handler.confluence_handler import ConfluenceHandler
from mindsdb.integrations.handlers.confluence_handler.confluence_tables import (
    ConfluenceBlogPostsTable,
    ConfluenceDatabasesTable,
    ConfluencePagesTable,
    ConfluenceSpacesTable,
    ConfluenceWhiteboardsTable,
    ConfluenceTasksTable,
)
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE
)
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator
)


class TestConfluenceHandler(BaseHandlerTestSetup, unittest.TestCase):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            api_base='https://demo.atlassian.net/',
            username='demo@example.com',
            password='demo_password',
        )

    def create_handler(self):
        return ConfluenceHandler('confluence', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('requests.Session')

    def test_connect(self):
        """
        Test if `connect` method successfully establishes a connection and sets `is_connected` flag to True.
        The `connect` method for this handler does not check the validity of the connection; it succeeds even with incorrect credentials.
        The `check_connection` method handles the connection status.
        """
        connection = self.handler.connect()

        self.assertIsNotNone(connection)
        self.assertTrue(self.handler.is_connected)

    def test_check_connection_success(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a successful connection.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: dict(
                results=[],
                _links=dict(next=None)
            ),
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)

        response = self.handler.check_connection()

        self.assertTrue(response.success)
        assert isinstance(response, StatusResponse)
        self.assertFalse(response.error_message)

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view",
                "limit": 1
            },
            json=None
        )

    def test_check_connection_failure(self):
        """
        Test that the `check_connection` method returns a StatusResponse object and accurately reflects the connection status on a failed connection.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=401,
            raise_for_status=lambda: None,
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)
        response = self.handler.check_connection()

        self.assertFalse(response.success)
        assert isinstance(response, StatusResponse)
        self.assertTrue(response.error_message)

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view",
                "limit": 1
            },
            json=None
        )

    def test_get_tables(self):
        """
        Test that the `get_tables` method returns a list of table names.
        """
        response = self.handler.get_tables()

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ['table_name', 'table_type'])

    def test_get_columns(self):
        """
        Test that the `get_columns` method returns a list of columns for a table.
        """
        response = self.handler.get_columns('spaces')

        self.assertIsInstance(response, Response)
        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        self.assertEqual(response.data_frame.columns.tolist(), ['Field', 'Type'])


class ConfluenceTablesTestSetup(BaseAPIResourceTestSetup):

    @property
    def dummy_connection_data(self):
        return OrderedDict(
            api_base='https://demo.atlassian.net/',
            username='demo@example.com',
            password='demo_password',
        )

    def create_handler(self):
        return ConfluenceHandler('confluence', connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch('requests.Session')

    def setUp(self):
        """
        Set up common test fixtures.
        """
        super().setUp()

        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: dict(
                results=[{column: f"mock_{column}" for column in self.resource.get_columns()}],
                _links=dict(next=None)
            )
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)


class TestConfluenceSpacesTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluenceSpacesTable(self.handler)

    def test_list_all_returns_results(self):
        """
        Test that the `list` with a query equivalent to `SELECT * FROM spaces` returns a list of spaces.
        """
        df = self.resource.list(
            conditions=[]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view"
            },
            json=None
        )

    def test_list_with_conditions_returns_results(self):
        """
        Test that the `list` method returns a list of spaces with the specified conditions.
        """
        mock_id = 'mock_id'
        mock_key = 'mock_key'
        mock_type = 'mock_type'
        mock_status = 'mock_status'
        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_id
                ),
                FilterCondition(
                    column="key",
                    op=FilterOperator.EQUAL,
                    value=mock_key
                ),
                FilterCondition(
                    column="type",
                    op=FilterOperator.EQUAL,
                    value=mock_type
                ),
                FilterCondition(
                    column="status",
                    op=FilterOperator.EQUAL,
                    value=mock_status
                ),
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/spaces",
            params={
                "description-format": "view",
                "ids": [mock_id],
                "keys": [mock_key],
                "type": mock_type,
                "status": mock_status
            },
            json=None
        )


class TestConfluencePagesTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluencePagesTable(self.handler)

    def test_list_all_returns_results(self):
        """
        Test that the `list` with a query equivalent to `SELECT * FROM pages` returns a list of pages.
        """
        df = self.resource.list(
            conditions=[]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/pages",
            params={
                "body-format": "storage"
            },
            json=None
        )

    def test_list_with_conditions_returns_results(self):
        """
        Test that the `list` method returns a list of pages with the specified conditions.
        """
        mock_id = 'mock_id'
        mock_space_id = 'mock_space_id'
        mock_status = 'mock_status'
        mock_title = 'mock_title'
        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_id
                ),
                FilterCondition(
                    column="spaceId",
                    op=FilterOperator.EQUAL,
                    value=mock_space_id
                ),
                FilterCondition(
                    column="status",
                    op=FilterOperator.EQUAL,
                    value=mock_status
                ),
                FilterCondition(
                    column="title",
                    op=FilterOperator.EQUAL,
                    value=mock_title
                )
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/pages",
            params={
                "body-format": "storage",
                "id": [mock_id],
                "space-id": [mock_space_id],
                "status": [mock_status],
                "title": mock_title
            },
            json=None
        )


class TestConfluenceBlogPostsTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluenceBlogPostsTable(self.handler)

    def test_list_all_returns_results(self):
        """
        Test that the `list` with a query equivalent to `SELECT * FROM blogposts` returns a list of blog posts.
        """
        df = self.resource.list(
            conditions=[]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/blogposts",
            params={
                "body-format": "storage"
            },
            json=None
        )

    def test_list_with_conditions_returns_results(self):
        """
        Test that the `list` method returns a list of blog posts with the specified conditions.
        """
        mock_id = 'mock_id'
        mock_space_id = 'mock_space_id'
        mock_status = 'mock_status'
        mock_title = 'mock_title'
        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_id
                ),
                FilterCondition(
                    column="spaceId",
                    op=FilterOperator.EQUAL,
                    value=mock_space_id
                ),
                FilterCondition(
                    column="status",
                    op=FilterOperator.EQUAL,
                    value=mock_status
                ),
                FilterCondition(
                    column="title",
                    op=FilterOperator.EQUAL,
                    value=mock_title
                )
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/blogposts",
            params={
                "body-format": "storage",
                "id": [mock_id],
                "space-id": [mock_space_id],
                "status": [mock_status],
                "title": mock_title
            },
            json=None
        )


class TestConfluenceDatabasesTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluenceDatabasesTable(self.handler)

    def test_list_with_database_id_returns_results(self):
        """
        Test that the `list` method returns a list of databases with the specified database ID.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {column: f"mock_{column}" for column in self.resource.get_columns()}
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)

        mock_id = 'mock_id'
        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_id
                ),
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/databases/{mock_id}",
            params=None,
            json=None
        )

    def test_list_without_database_id_raises_error(self):
        """
        Test that the `list` method raises an error when no database ID is provided.
        """
        with self.assertRaises(ValueError):
            self.resource.list(conditions=[])


class TestConfluenceWhiteboardsTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluenceWhiteboardsTable(self.handler)

    def test_list_with_whiteboard_id_returns_results(self):
        """
        Test that the `list` method returns a list of whiteboards with the specified whiteboard ID.
        """
        mock_request = MagicMock()
        mock_request.return_value = MagicMock(
            status_code=200,
            raise_for_status=lambda: None,
            json=lambda: {column: f"mock_{column}" for column in self.resource.get_columns()}
        )
        self.mock_connect.return_value = MagicMock(request=mock_request)

        mock_id = 'mock_id'
        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_id
                ),
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/whiteboards/{mock_id}",
            params=None,
            json=None
        )

    def test_list_without_whiteboard_id_raises_error(self):
        """
        Test that the `list` method raises an error when no whiteboard ID is provided.
        """
        with self.assertRaises(ValueError):
            self.resource.list(conditions=[])


class TestConfluenceTasksTable(ConfluenceTablesTestSetup, unittest.TestCase):

    def create_resource(self):
        return ConfluenceTasksTable(self.handler)

    def test_list_all_returns_results(self):
        """
        Test that the `list` with a query equivalent to `SELECT * FROM tasks` returns a list of tasks.
        """
        df = self.resource.list(
            conditions=[]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/tasks",
            params={
                "body-format": "storage"
            },
            json=None
        )

    def test_list_with_conditions_returns_results(self):
        """
        Test that the `list` method returns a list of tasks with the specified conditions.
        """
        mock_task_ids = ['mock_task_id']
        mock_space_ids = ['mock_space_id']
        mock_page_ids = ['mock_page_id']
        mock_created_by_ids = ['mock_created_by_id']
        mock_assigned_to_ids = ['mock_assigned_to_id']
        mock_completed_by_ids = ['mock_completed_by_id']
        mock_status = 'mock_status'

        df = self.resource.list(
            conditions=[
                FilterCondition(
                    column="id",
                    op=FilterOperator.EQUAL,
                    value=mock_task_ids[0]
                ),
                FilterCondition(
                    column="spaceId",
                    op=FilterOperator.EQUAL,
                    value=mock_space_ids[0]
                ),
                FilterCondition(
                    column="pageId",
                    op=FilterOperator.EQUAL,
                    value=mock_page_ids[0]
                ),
                FilterCondition(
                    column="createdBy",
                    op=FilterOperator.EQUAL,
                    value=mock_created_by_ids[0]
                ),
                FilterCondition(
                    column="assignedTo",
                    op=FilterOperator.EQUAL,
                    value=mock_assigned_to_ids[0]
                ),
                FilterCondition(
                    column="completedBy",
                    op=FilterOperator.EQUAL,
                    value=mock_completed_by_ids[0]
                ),
                FilterCondition(
                    column="status",
                    op=FilterOperator.EQUAL,
                    value=mock_status
                ),
            ]
        )

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.columns.tolist(), self.resource.get_columns())
        self.assertEqual(df.shape, (1, len(self.resource.get_columns())))

        self.mock_connect.return_value.request.assert_called_with(
            "GET",
            f"{self.dummy_connection_data['api_base']}/wiki/api/v2/tasks",
            params={
                "body-format": "storage",
                "id": mock_task_ids,
                "space-id": mock_space_ids,
                "page-id": mock_page_ids,
                "created-by": mock_created_by_ids,
                "assigned-to": mock_assigned_to_ids,
                "completed-by": mock_completed_by_ids,
                "status": mock_status
            },
            json=None
        )


if __name__ == '__main__':
    unittest.main()

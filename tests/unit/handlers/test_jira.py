import pytest
import unittest
import datetime
from array import array
from decimal import Decimal
from collections import OrderedDict
from unittest.mock import patch, MagicMock
from requests.exceptions import HTTPError


from base_handler_test import BaseDatabaseHandlerTest

from mindsdb.integrations.handlers.jira_handler.jira_handler import JiraHandler


class TestJiraHandler(BaseDatabaseHandlerTest, unittest.TestCase):

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

    def get_tables_query(self):
        pass

    def get_columns_query(self, table_name):
        pass

    def test_connect_cloud_success(self):
        pass

    def test_check_connection_success(self):
        pass

    def test_check_connection_failure(self):
        pass

    def test_register_tables(self):
        pass

    def test_native_query_execution(self):
        pass

    def test_get_tables(self):
        pass

    def test_get_columns(self):
        pass


if __name__ == "__main__":
    unittest.main()

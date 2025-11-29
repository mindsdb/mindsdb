import pytest
import unittest
import datetime
from array import array
from decimal import Decimal
from collections import OrderedDict
from unittest.mock import patch, MagicMock

from base_handler_test import BaseDatabaseHandlerTest


class TestJiraHandler(BaseDatabaseHandlerTest, unittest.TestCase):
    handler_name = "jira"
    required_connection_keys = ["jira_url", "username", "api_token"]
    default_tables = [
        "projects",
        "issues",
        "users",
        "groups",
        "attachments",
        "comments",
    ]

    @pytest.fixture(autouse=True)
    def _setup_handler(self):
        self.handler_setup()

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

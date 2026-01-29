from collections import OrderedDict
import unittest
from unittest.mock import MagicMock, patch

import pytest

try:
    from mindsdb.integrations.handlers.netsuite_handler.netsuite_handler import NetSuiteHandler
    from mindsdb.integrations.handlers.netsuite_handler.netsuite_tables import NetSuiteRecordTable
except ImportError:
    pytestmark = pytest.mark.skip("NetSuite handler not installed")

from base_handler_test import BaseHandlerTestSetup
from mindsdb.integrations.libs.response import RESPONSE_TYPE
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator


class TestNetSuiteHandler(BaseHandlerTestSetup, unittest.TestCase):
    @property
    def dummy_connection_data(self):
        return OrderedDict(
            account_id="123456_SB1",
            consumer_key="ck",
            consumer_secret="cs",
            token_id="token",
            token_secret="secret",
            record_types="customer",
        )

    def create_handler(self):
        return NetSuiteHandler("netsuite", connection_data=self.dummy_connection_data)

    def create_patcher(self):
        return patch("mindsdb.integrations.handlers.netsuite_handler.netsuite_handler.requests.Session")

    def test_connect_success(self):
        session = MagicMock()
        self.mock_connect.return_value = session

        with patch("mindsdb.integrations.handlers.netsuite_handler.netsuite_handler.OAuth1") as oauth_mock:
            connection = self.handler.connect()

        self.assertIs(connection, session)
        self.assertTrue(self.handler.is_connected)
        oauth_mock.assert_called_once()
        self.assertEqual(session.auth, oauth_mock.return_value)

    def test_connect_missing_required_params_raises(self):
        handler = NetSuiteHandler("netsuite", connection_data={"account_id": "123"})
        with self.assertRaises(ValueError):
            handler.connect()

    def test_check_connection_success(self):
        with patch("mindsdb.integrations.handlers.netsuite_handler.netsuite_handler.OAuth1"):
            self.handler._request = MagicMock()
            response = self.handler.check_connection()

        self.assertTrue(response.success)
        self.handler._request.assert_called_once_with("POST", "/services/rest/query/v1/suiteql", json={"q": "SELECT 1"})

    def test_check_connection_failure_sets_error_message(self):
        with patch("mindsdb.integrations.handlers.netsuite_handler.netsuite_handler.OAuth1"):
            self.handler._request = MagicMock(side_effect=RuntimeError("boom"))
            response = self.handler.check_connection()

        self.assertFalse(response.success)
        self.assertEqual(response.error_message, "boom")

    def test_native_query_parses_suiteql_payload(self):
        payload = {
            "columnMetadata": [{"name": "id"}, {"name": "id"}],
            "items": [{"values": [1, "a"]}, {"values": [2]}],
        }
        self.handler._request = MagicMock(return_value=payload)

        response = self.handler.native_query("SELECT id FROM transaction")

        self.assertEqual(response.type, RESPONSE_TYPE.TABLE)
        df = response.data_frame
        self.assertEqual(list(df.columns), ["id", "id_2"])
        self.assertEqual(df.iloc[0].tolist(), [1, "a"])
        self.assertEqual(df.iloc[1].tolist(), [2, None])


class TestNetSuiteRecordTable(unittest.TestCase):
    def test_list_builds_q_filters(self):
        handler = MagicMock()
        handler._request.return_value = {"items": [], "links": []}

        table = NetSuiteRecordTable(handler, "customer")
        conditions = [
            FilterCondition("email", FilterOperator.EQUAL, "user@example.com"),
            FilterCondition("foo", FilterOperator.EQUAL, None),
        ]

        table.list(conditions=conditions, limit=10)

        args, kwargs = handler._request.call_args
        self.assertEqual(args[0], "GET")
        self.assertIn("params", kwargs)
        self.assertEqual(
            kwargs["params"]["q"],
            "email = 'user@example.com' AND foo IS NULL",
        )

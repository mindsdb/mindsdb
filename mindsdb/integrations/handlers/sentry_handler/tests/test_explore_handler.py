import json
import unittest
from unittest.mock import Mock

from mindsdb.integrations.handlers.sentry_handler.explore.client import ExploreClient
from mindsdb.integrations.handlers.sentry_handler.explore.errors import (
    ExploreAuthenticationError,
    ExploreCapabilityError,
    ExplorePermissionError,
    ExploreQueryError,
)
from mindsdb.integrations.handlers.sentry_handler.explore.handler import ExploreSentryHandler
from mindsdb.integrations.handlers.sentry_handler.explore.models import (
    ExploreDataset,
    ExploreTableRequest,
)
from mindsdb.integrations.handlers.sentry_handler.explore.sql import build_logs_request
from mindsdb.integrations.handlers.sentry_handler.explore.tables import (
    SentryLogsTable,
    SentryLogsTimeseriesTable,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_client import SentryRequestError
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn,
)


class MockResponse:
    def __init__(self, status_code, payload, headers=None, text="") -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class HandlerStub:
    def __init__(self, client) -> None:
        self.client = client

    def connect(self):
        return self.client


class ExploreSentryTablesTest(unittest.TestCase):
    def test_explore_handler_builds_logs_request_and_flattens_rows(self):
        base_client = Mock()
        base_client.organization_slug = "talentify"
        base_client.resolve_project.return_value = {"id": 99, "slug": "mktplace"}
        base_client.request_json.return_value = (
            {
                "data": [
                    {
                        "timestamp": "2026-03-18T10:00:00Z",
                        "severity": "error",
                        "message": "Refresh token expired",
                        "trace.id": "trace-1",
                        "span.id": "span-1",
                        "sentry.release": "2026.3.18",
                        "logger.name": "auth.worker",
                        "attributes": {
                            "member_id": "member-1",
                            "org_id": "org-1",
                        },
                    }
                ]
            },
            MockResponse(200, {}),
        )
        handler = ExploreSentryHandler(
            "sentry",
            {"environment": "production"},
            issue_handler=HandlerStub(base_client),
        )
        table = SentryLogsTable(handler)
        conditions = [
            FilterCondition("level", FilterOperator.EQUAL, "error"),
            FilterCondition("message", FilterOperator.LIKE, "%token%"),
            FilterCondition("timestamp", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-03-18"),
        ]

        df = table.list(
            conditions=conditions,
            limit=50,
            sort=[SortColumn("timestamp", ascending=False)],
            targets=["timestamp", "message", "level", "project_slug"],
        )

        base_client.request_json.assert_called_once()
        _, path = base_client.request_json.call_args.args[:2]
        params = base_client.request_json.call_args.kwargs["params"]

        self.assertEqual("/organizations/talentify/events/", path)
        self.assertEqual("logs", params["dataset"])
        self.assertEqual([99], params["project"])
        self.assertEqual(["production"], params["environment"])
        self.assertEqual(sorted(["timestamp", "message", "severity"]), sorted(params["field"]))
        self.assertEqual("-timestamp", params["sort"])
        self.assertIn("severity:error", params["query"])
        self.assertIn("*token*", params["query"])
        self.assertIsNone(params.get("statsPeriod"))
        self.assertEqual("mktplace", df.iloc[0]["project_slug"])
        self.assertEqual("production", df.iloc[0]["environment"])
        self.assertEqual("auth.worker", df.iloc[0]["logger"])
        self.assertEqual(
            {"member_id": "member-1", "org_id": "org-1"},
            json.loads(df.iloc[0]["extra_json"]),
        )
        self.assertTrue(all(condition.applied for condition in conditions))

    def test_explore_handler_builds_logs_timeseries_request(self):
        base_client = Mock()
        base_client.organization_slug = "talentify"
        base_client.resolve_project.return_value = {"id": 99, "slug": "mktplace"}
        base_client.request_json.return_value = (
            {
                "timeSeries": [
                    {
                        "values": [
                            {"timestamp": 1710763200000, "value": 5, "incomplete": False},
                            {"timestamp": 1710849600000, "value": 7, "incomplete": False},
                        ]
                    }
                ]
            },
            MockResponse(200, {}),
        )
        handler = ExploreSentryHandler(
            "sentry",
            {"environment": "production"},
            issue_handler=HandlerStub(base_client),
        )
        table = SentryLogsTimeseriesTable(handler)
        conditions = [FilterCondition("level", FilterOperator.EQUAL, "error")]

        df = table.list(conditions=conditions, limit=10)

        base_client.request_json.assert_called_once()
        _, path = base_client.request_json.call_args.args[:2]
        params = base_client.request_json.call_args.kwargs["params"]

        self.assertEqual("/organizations/talentify/events-timeseries/", path)
        self.assertEqual("logs", params["dataset"])
        self.assertEqual("count()", params["yAxis"])
        self.assertEqual(86400, params["interval"])
        self.assertEqual("7d", params["statsPeriod"])
        self.assertEqual("severity:error", params["query"])
        self.assertEqual(["production"], params["environment"])
        self.assertEqual(2, len(df))
        self.assertEqual(["bucket_start", "value"], list(df.columns))

    def test_build_logs_request_translates_message_like_to_explore_search(self):
        conditions = [FilterCondition("message", FilterOperator.LIKE, "%token%")]

        request = build_logs_request(
            project_id=99,
            environment="production",
            conditions=conditions,
            limit=20,
            sort=[SortColumn("timestamp", ascending=False)],
            targets=["timestamp", "message", "level"],
        )

        self.assertEqual("*token*", request.query)
        self.assertTrue(conditions[0].applied)


class ExploreClientErrorHandlingTest(unittest.TestCase):
    def setUp(self):
        self.sentry_client = Mock()
        self.sentry_client.organization_slug = "talentify"
        self.client = ExploreClient(sentry_client=self.sentry_client, environment="production")
        self.request = ExploreTableRequest(
            dataset=ExploreDataset.LOGS,
            fields=["timestamp", "message"],
            query="severity:error",
            limit=20,
            sort="-timestamp",
            start=None,
            end=None,
            stats_period="7d",
            project_ids=[99],
            environments=["production"],
        )

    def test_query_table_maps_401_to_authentication_error(self):
        self.sentry_client.request_json.side_effect = SentryRequestError(
            "Sentry explore logs table request failed with status 401",
            operation="explore logs table",
            status_code=401,
        )

        with self.assertRaises(ExploreAuthenticationError):
            self.client.query_table(self.request)

    def test_query_table_maps_403_to_permission_error(self):
        self.sentry_client.request_json.side_effect = SentryRequestError(
            "Sentry explore logs table request failed with status 403",
            operation="explore logs table",
            status_code=403,
        )

        with self.assertRaises(ExplorePermissionError):
            self.client.query_table(self.request)

    def test_query_table_maps_404_to_capability_error(self):
        self.sentry_client.request_json.side_effect = SentryRequestError(
            "Sentry explore logs table request failed with status 404",
            operation="explore logs table",
            status_code=404,
        )

        with self.assertRaises(ExploreCapabilityError):
            self.client.query_table(self.request)

    def test_query_table_raises_query_error_for_other_failures(self):
        self.sentry_client.request_json.side_effect = SentryRequestError(
            "Sentry explore logs table request failed with status 400",
            operation="explore logs table",
            status_code=400,
        )

        with self.assertRaises(ExploreQueryError):
            self.client.query_table(self.request)

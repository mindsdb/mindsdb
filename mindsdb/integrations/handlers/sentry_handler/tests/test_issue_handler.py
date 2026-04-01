import unittest
from datetime import date
from unittest.mock import Mock, patch

import pandas as pd
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.sentry_handler.issue.tables import (
    SentryIssuesTable,
    SentryProjectsTable,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_client import (
    SentryClient,
    SentryRequestError,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_handler import SentryHandler
from mindsdb.integrations.utilities.sql_utils import (
    FilterCondition,
    FilterOperator,
    SortColumn,
    extract_comparison_conditions,
)


class MockResponse:
    def __init__(self, status_code, payload, headers=None, text="") -> None:
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text

    def json(self):
        return self._payload


class MockSession:
    def __init__(self, responses) -> None:
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def request(self, method, url, params=None, timeout=None):
        self.calls.append({"method": method, "url": url, "params": params, "timeout": timeout})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class HandlerStub:
    def __init__(self, client) -> None:
        self.client = client

    def connect(self):
        return self.client


class SentryClientTest(unittest.TestCase):
    def test_list_projects_paginates_and_respects_cursor(self):
        session = MockSession(
            [
                MockResponse(
                    200,
                    [{"id": "1", "slug": "core"}],
                    headers={
                        "Link": '<https://sentry.io/api/0/organizations/talentify/projects/?cursor=abc>; '
                        'results="true"; rel="next"'
                    },
                ),
                MockResponse(200, [{"id": "2", "slug": "mktplace"}]),
            ]
        )
        client = SentryClient(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            session=session,
            sleep=lambda _: None,
        )

        projects = client.list_projects()

        self.assertEqual(2, len(projects))
        self.assertEqual("abc", session.calls[1]["params"]["cursor"])

    def test_request_retries_retryable_status_codes(self):
        session = MockSession(
            [
                MockResponse(429, {"detail": "rate limited"}),
                MockResponse(200, [{"id": "2", "slug": "mktplace"}]),
            ]
        )
        client = SentryClient(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            session=session,
            sleep=lambda _: None,
        )

        projects = client.list_projects(limit=1)

        self.assertEqual([{"id": "2", "slug": "mktplace"}], projects)
        self.assertEqual(2, len(session.calls))

    def test_request_json_retries_transient_server_errors(self):
        session = MockSession(
            [
                MockResponse(503, {"detail": "temporarily unavailable"}),
                MockResponse(200, {"data": [{"message": "ok"}]}),
            ]
        )
        client = SentryClient(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            session=session,
            sleep=lambda _: None,
        )

        payload, _ = client.request_json(
            "GET",
            "/organizations/talentify/events/",
            params={"dataset": "logs"},
            operation="explore logs table",
        )

        self.assertEqual({"data": [{"message": "ok"}]}, payload)
        self.assertEqual(2, len(session.calls))

    def test_list_issues_prepends_environment_query_fragment(self):
        session = MockSession([MockResponse(200, [])])
        client = SentryClient(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            session=session,
            sleep=lambda _: None,
        )

        client.list_issues(project_id=99, query="status:unresolved level:error", limit=1)

        self.assertEqual(
            'environment:"production" status:unresolved level:error',
            session.calls[0]["params"]["query"],
        )

    def test_list_issues_uses_environment_query_when_query_is_empty(self):
        session = MockSession([MockResponse(200, [])])
        client = SentryClient(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            session=session,
            sleep=lambda _: None,
        )

        client.list_issues(project_id=99, query="", limit=1)

        self.assertEqual('environment:"production"', session.calls[0]["params"]["query"])


class SentryHandlerTest(unittest.TestCase):
    def test_validate_connection_requires_environment(self):
        handler = SentryHandler(
            "sentry",
            {
                "auth_token": "token",
                "organization_slug": "talentify",
                "project_slug": "mktplace",
            },
        )

        with self.assertRaisesRegex(ValueError, "environment"):
            handler._validate_connection_data()

    def test_connect_passes_environment_to_client(self):
        handler = SentryHandler(
            "sentry",
            {
                "auth_token": "token",
                "organization_slug": "talentify",
                "project_slug": "mktplace",
                "environment": "production",
            },
        )

        with patch("mindsdb.integrations.handlers.sentry_handler.issue.handler.SentryClient") as client_cls:
            client = client_cls.return_value
            client.validate_connection.return_value = {"id": 99}

            handler.connect()

        client_cls.assert_called_once_with(
            auth_token="token",
            organization_slug="talentify",
            project_slug="mktplace",
            environment="production",
            base_url="https://sentry.io",
        )
        client.validate_connection.assert_called_once_with()


class SentryTablesTest(unittest.TestCase):
    def test_projects_table_flattens_project_payload(self):
        client = Mock()
        client.list_projects.return_value = [
            {
                "id": "17",
                "slug": "mktplace",
                "name": "Marketplace",
                "platform": "python",
                "dateCreated": "2026-03-01T10:00:00Z",
                "latestRelease": {"version": "2026.3.1"},
            }
        ]
        table = SentryProjectsTable(HandlerStub(client))

        df = table.list(limit=10)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(
            {
                "project_id": 17,
                "project_slug": "mktplace",
                "project_name": "Marketplace",
                "platform": "python",
                "date_created": "2026-03-01T10:00:00Z",
                "latest_release": "2026.3.1",
            },
            df.iloc[0].to_dict(),
        )

    def test_issues_table_builds_structured_query_and_flattens_rows(self):
        client = Mock()
        client.resolve_project.return_value = {"id": 99, "slug": "mktplace"}
        client.list_issues.return_value = [
            {
                "id": "1001",
                "shortId": "MKTPLACE-1",
                "title": "Checkout failed",
                "type": "error",
                "culprit": "checkout.views.create_order",
                "status": "unresolved",
                "level": "error",
                "count": "12",
                "userCount": "7",
                "firstSeen": "2026-03-17T10:00:00Z",
                "lastSeen": "2026-03-18T10:00:00Z",
                "permalink": "https://sentry.io/organizations/talentify/issues/1001/",
            }
        ]
        table = SentryIssuesTable(HandlerStub(client))
        conditions = [FilterCondition("status", FilterOperator.EQUAL, "unresolved")]

        df = table.list(conditions=conditions, limit=20, sort=[SortColumn("count", ascending=False)])

        client.list_issues.assert_called_once_with(project_id=99, query="status:unresolved", limit=1000)
        self.assertTrue(conditions[0].applied)
        self.assertEqual("Checkout failed", df.iloc[0]["title"])
        self.assertEqual(12, df.iloc[0]["count"])
        self.assertNotIn("environment", df.columns)

    def test_issues_table_rejects_query_and_structured_filters_together(self):
        table = SentryIssuesTable(HandlerStub(Mock()))
        conditions = [
            FilterCondition("query", FilterOperator.EQUAL, "is:unresolved"),
            FilterCondition("status", FilterOperator.EQUAL, "unresolved"),
        ]

        with self.assertRaisesRegex(
            ValueError,
            "Sentry issues query filter cannot be combined with status or level filters",
        ):
            table._build_query_string(conditions)

    def test_issues_table_applies_local_last_seen_filter(self):
        client = Mock()
        client.resolve_project.return_value = {"id": 99, "slug": "mktplace"}
        client.list_issues.return_value = [
            {
                "id": "1001",
                "shortId": "MKTPLACE-1",
                "title": "Today issue",
                "type": "error",
                "culprit": "checkout.views.create_order",
                "status": "unresolved",
                "level": "error",
                "count": "12",
                "userCount": "7",
                "firstSeen": "2026-03-17T10:00:00Z",
                "lastSeen": "2026-03-18T10:00:00Z",
                "permalink": "https://sentry.io/organizations/talentify/issues/1001/",
            },
            {
                "id": "1002",
                "shortId": "MKTPLACE-2",
                "title": "Old issue",
                "type": "error",
                "culprit": "checkout.views.create_order",
                "status": "unresolved",
                "level": "error",
                "count": "3",
                "userCount": "2",
                "firstSeen": "2026-03-16T10:00:00Z",
                "lastSeen": "2026-03-17T10:00:00Z",
                "permalink": "https://sentry.io/organizations/talentify/issues/1002/",
            },
        ]
        table = SentryIssuesTable(HandlerStub(client))
        conditions = [
            FilterCondition("level", FilterOperator.EQUAL, "error"),
            FilterCondition("last_seen", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-03-18"),
        ]

        df = table.list(conditions=conditions, limit=20, sort=[SortColumn("last_seen", ascending=False)])

        self.assertEqual(["Today issue"], list(df["title"]))
        self.assertTrue(conditions[0].applied)
        self.assertTrue(conditions[1].applied)

    def test_extract_comparison_conditions_supports_current_date_and_date_literal(self):
        current_date_where = parse_sql("SELECT * FROM issues WHERE last_seen::DATE = CURRENT_DATE").where
        literal_date_where = parse_sql("SELECT * FROM issues WHERE last_seen >= '2026-03-18'").where

        self.assertEqual([["=", "last_seen", date.today().isoformat()]], extract_comparison_conditions(current_date_where))
        self.assertEqual([[">=", "last_seen", "2026-03-18"]], extract_comparison_conditions(literal_date_where))

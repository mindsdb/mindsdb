import unittest
from unittest.mock import Mock

import pandas as pd

from mindsdb.integrations.handlers.sentry_handler.sentry_client import SentryClient
from mindsdb.integrations.handlers.sentry_handler.sentry_tables import (
    SentryIssuesTable,
    SentryProjectsTable,
)
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, SortColumn


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
            session=session,
            sleep=lambda _: None,
        )

        projects = client.list_projects(limit=1)

        self.assertEqual([{"id": "2", "slug": "mktplace"}], projects)
        self.assertEqual(2, len(session.calls))


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
                "environments": ["production"],
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
        self.assertEqual("production", df.iloc[0]["environment"])

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

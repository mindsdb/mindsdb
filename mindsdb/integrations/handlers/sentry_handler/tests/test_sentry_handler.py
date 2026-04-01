import unittest

from mindsdb.integrations.handlers.sentry_handler import sentry_tables
from mindsdb.integrations.handlers.sentry_handler.explore.tables import (
    SentryLogsTable,
    SentryLogsTimeseriesTable,
)
from mindsdb.integrations.handlers.sentry_handler.issue.handler import IssueSentryHandler
from mindsdb.integrations.handlers.sentry_handler.issue.tables import (
    SentryIssuesTable,
    SentryProjectsTable,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_handler import SentryHandler


class SentryHandlerCompatibilityTest(unittest.TestCase):
    def test_public_handler_entrypoint_inherits_issue_handler(self):
        handler = SentryHandler(
            "sentry",
            {
                "auth_token": "token",
                "organization_slug": "talentify",
                "project_slug": "mktplace",
                "environment": "production",
            },
        )

        self.assertIsInstance(handler, IssueSentryHandler)
        self.assertIn("projects", handler._tables)
        self.assertIn("issues", handler._tables)
        self.assertIn("logs", handler._tables)
        self.assertIn("logs_timeseries", handler._tables)

    def test_legacy_tables_module_reexports_issue_and_explore_tables(self):
        self.assertIs(sentry_tables.SentryProjectsTable, SentryProjectsTable)
        self.assertIs(sentry_tables.SentryIssuesTable, SentryIssuesTable)
        self.assertIs(sentry_tables.SentryLogsTable, SentryLogsTable)
        self.assertIs(sentry_tables.SentryLogsTimeseriesTable, SentryLogsTimeseriesTable)

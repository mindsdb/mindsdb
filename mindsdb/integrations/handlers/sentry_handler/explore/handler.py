from __future__ import annotations

from typing import Any

from mindsdb.integrations.handlers.sentry_handler.explore.client import ExploreClient
from mindsdb.integrations.handlers.sentry_handler.explore.tables import (
    SentryLogsTable,
    SentryLogsTimeseriesTable,
)
from mindsdb.integrations.libs.api_handler import APIHandler


class ExploreSentryHandler(APIHandler):
    name = "sentry_explore"

    def __init__(
        self,
        name: str,
        connection_data: dict[str, Any],
        *,
        issue_handler: Any,
    ) -> None:
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.issue_handler = issue_handler
        self.environment = self.connection_data["environment"]
        self.connection: ExploreClient | None = None
        self.is_connected = False
        self.thread_safe = True

        self._register_table("logs", SentryLogsTable(self))
        self._register_table("logs_timeseries", SentryLogsTimeseriesTable(self))

    def connect(self) -> ExploreClient:
        if self.is_connected and self.connection is not None:
            return self.connection

        sentry_client = self.issue_handler.connect()
        self.connection = ExploreClient(
            sentry_client=sentry_client,
            environment=self.environment,
        )
        self.is_connected = True
        return self.connection

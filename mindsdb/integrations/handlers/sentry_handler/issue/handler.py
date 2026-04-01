from __future__ import annotations

from typing import Any

from mindsdb.integrations.handlers.sentry_handler.issue.tables import (
    SentryIssuesTable,
    SentryProjectsTable,
)
from mindsdb.integrations.handlers.sentry_handler.sentry_client import SentryClient
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse as StatusResponse
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql


logger = log.getLogger(__name__)


class IssueSentryHandler(APIHandler):
    """Issue-focused Sentry handler used by the public sentry connector entrypoint."""

    name = "sentry"

    def __init__(self, name: str, connection_data: dict[str, Any], **kwargs: Any) -> None:
        super().__init__(name)
        self.connection_data = connection_data or {}
        self.kwargs = kwargs

        self.connection: SentryClient | None = None
        self.is_connected = False
        self.thread_safe = True

        self._register_table("projects", SentryProjectsTable(self))
        self._register_table("issues", SentryIssuesTable(self))

    def connect(self) -> SentryClient:
        if self.is_connected and self.connection is not None:
            return self.connection

        self._validate_connection_data()
        self.connection = SentryClient(
            auth_token=self.connection_data["auth_token"],
            organization_slug=self.connection_data["organization_slug"],
            project_slug=self.connection_data["project_slug"],
            environment=self.connection_data["environment"],
            base_url=self.connection_data.get("base_url") or "https://sentry.io",
        )
        self.connection.validate_connection()
        self.is_connected = True
        return self.connection

    def check_connection(self) -> StatusResponse:
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f"Error connecting to Sentry API: {e}!")
            response.error_message = e

        self.is_connected = response.success
        return response

    def native_query(self, query: str) -> StatusResponse:
        ast = parse_sql(query)
        return self.query(ast)

    def _validate_connection_data(self) -> None:
        required_keys = ["auth_token", "organization_slug", "project_slug", "environment"]
        missing = [key for key in required_keys if not self.connection_data.get(key)]
        if missing:
            raise ValueError(
                "Required parameters must be provided and should not be empty: "
                + ", ".join(sorted(missing))
            )

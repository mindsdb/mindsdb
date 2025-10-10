from __future__ import annotations

from typing import Any, Dict, Optional

from mindsdb.utilities import log
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)
from mindsdb_sql_parser import parse_sql

from mindsdb.integrations.handlers.email_handler.email_tables import EmailsTable
from mindsdb.integrations.handlers.email_handler.email_client import EmailClient
from mindsdb.integrations.handlers.email_handler.settings import EmailConnectionDetails

logger = log.getLogger(__name__)


class EmailHandler(APIHandler):
    """
    Handler for interacting with Email (send and search).

    Parameters
    ----------
    name : Optional[str]
        The handler name
    connection_data : Dict[str, Any]
        Connection details; see EmailConnectionDetails for all fields.
    """

    def __init__(self, name: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(name)

        raw_connection_data: Dict[str, Any] = kwargs.get("connection_data", {}) or {}
        # Construct a validated settings model. This also ensures forward/backward compatibility.
        self.connection_data = EmailConnectionDetails(**raw_connection_data)
        self.kwargs = kwargs

        self.connection: Optional[EmailClient] = None
        self.is_connected = False

        emails = EmailsTable(self)
        self._register_table("emails", emails)

    def connect(self) -> EmailClient:
        """Create a client instance using connection settings."""
        if self.is_connected and self.connection is not None:
            return self.connection

        try:
            self.connection = EmailClient(self.connection_data)
            self.is_connected = True
        except Exception as e:
            logger.error(f"Error initializing Email client: {e}")
            # Keep consistent state on failure
            self.connection = None
            self.is_connected = False
            raise

        return self.connection

    def disconnect(self) -> None:
        """Close any existing connections and reset flags."""
        if self.connection is None:
            self.is_connected = False
            return

        try:
            self.connection.logout()
        except Exception as e:
            # Don't fail on disconnect; just log.
            logger.warning(f"Non-fatal error during email disconnect: {e}")
        finally:
            self.is_connected = False
            self.connection = None

    def check_connection(self) -> StatusResponse:
        """Validate connectivity and credentials."""
        response = StatusResponse(False)
        try:
            client = self.connect()
            # Light-weight capability check: rely on EmailClient.select_mailbox's internal sanitization.
            try:
                client.select_mailbox("INBOX")
            finally:
                # Be a good citizen: close the session created for this check.
                client.logout()
            response.success = True
        except Exception as e:
            response.error_message = f"Error connecting to Email: {e}."
            logger.error(response.error_message)
            # Ensure clean state
            self.disconnect()

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw SQL query."""
        ast = parse_sql(query)
        return self.query(ast)

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
from mindsdb.integrations.handlers.email_handler.email_client import _sanitize_mailbox
from mindsdb.integrations.handlers.email_handler.settings import EmailConnectionDetails

logger = log.getLogger(__name__)


class EmailHandler(APIHandler):
    """
    Handler for interacting with Email (send and search).
    """

    def __init__(self, name: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(name)

        raw_connection_data: Dict[str, Any] = kwargs.get("connection_data", {}) or {}
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
            self.connection = None
            self.is_connected = False
            raise

        return self.connection

    def disconnect(self) -> None:
        """Close any existing connections and reset flags."""
        try:
            if self.connection is not None:
                try:
                    self.connection.logout()
                except Exception as e:
                    logger.warning(f"Non-fatal error during email disconnect: {e}")
        finally:
            # Always reset state
            self.is_connected = False
            self.connection = None

    def check_connection(self) -> StatusResponse:
        """Validate connectivity and credentials with minimal overhead."""
        response = StatusResponse(False)
        created_here = False
        try:
            if not self.is_connected or self.connection is None:
                self.connection = self.connect()
                created_here = True
            client = self.connection
            assert client is not None

            # Explicitly sanitize mailbox before use
            inbox = _sanitize_mailbox("INBOX")
            client.select_mailbox(inbox)

            # Only teardown if we created it here (preserve reuse for callers)
            if created_here:
                client.logout()
                self.is_connected = False
                self.connection = None

            response.success = True
        except Exception as e:
            response.error_message = f"Error connecting to Email: {e}."
            logger.error(response.error_message)
            # Ensure clean state on error
            self.disconnect()
        finally:
            # If we created the connection here and it's still active, clean up
            if created_here and self.is_connected:
                self.disconnect()

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw SQL query."""
        ast = parse_sql(query)
        return self.query(ast)

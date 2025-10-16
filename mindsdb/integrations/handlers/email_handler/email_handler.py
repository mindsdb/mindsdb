from __future__ import annotations

import threading
from hashlib import sha256
from typing import Any, Dict, Optional, Tuple

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

    # Simple shared-connection pool keyed by effective connection parameters
    _POOL: Dict[Tuple, EmailClient] = {}
    _POOL_LOCK = threading.Lock()

    def __init__(self, name: Optional[str] = None, **kwargs: Any) -> None:
        super().__init__(name)

        raw_connection_data: Dict[str, Any] = kwargs.get("connection_data", {}) or {}
        self.connection_data = EmailConnectionDetails(**raw_connection_data)
        self.kwargs = kwargs

        self.connection: Optional[EmailClient] = None
        self.is_connected = False

        emails = EmailsTable(self)
        self._register_table("emails", emails)

    def _pool_key(self) -> Tuple:
        cd = self.connection_data
        pwd_hash = sha256((cd.password or "").encode("utf-8")).hexdigest()[:12]
        return (
            cd.resolved_imap_host,
            cd.resolved_imap_port,
            bool(cd.imap_use_ssl),
            bool(cd.imap_use_starttls),
            cd.resolved_imap_username,
            cd.resolved_smtp_host,
            cd.resolved_smtp_port,
            bool(cd.smtp_starttls),
            cd.email,
            pwd_hash,
        )

    def connect(self) -> EmailClient:
        """Create or reuse a client instance using connection settings."""
        if self.is_connected and self.connection is not None:
            return self.connection

        key = self._pool_key()
        with EmailHandler._POOL_LOCK:
            pooled = EmailHandler._POOL.get(key)
            if pooled is not None:
                # Reuse pooled connection
                self.connection = pooled
                self.is_connected = True
                return self.connection

            # Create and pool the connection
            self.connection = EmailClient(self.connection_data)
            EmailHandler._POOL[key] = self.connection
            self.is_connected = True

        return self.connection

    def disconnect(self) -> None:
        """Close any existing connections and reset flags. Force cleanup on failure."""
        try:
            if self.connection is not None:
                key = self._pool_key()
                try:
                    self.connection.logout()
                except Exception as e:
                    logger.warning(f"Non-fatal error during email logout: {e}")
                finally:
                    # Force close if logout left servers open
                    try:
                        if getattr(self.connection, "imap_server", None) is not None:
                            try:
                                self.connection.imap_server.logout()
                            except Exception:
                                pass
                            try:
                                self.connection.imap_server = None
                            except Exception:
                                pass
                        if getattr(self.connection, "smtp_server", None) is not None:
                            try:
                                self.connection.smtp_server.quit()
                            except Exception:
                                pass
                            try:
                                self.connection.smtp_server = None
                            except Exception:
                                pass
                    except Exception:
                        pass
                    # Remove from pool
                    with EmailHandler._POOL_LOCK:
                        EmailHandler._POOL.pop(key, None)
        finally:
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

            inbox = _sanitize_mailbox("INBOX")
            client.select_mailbox(inbox)

            response.success = True
        except Exception as e:
            response.error_message = f"Error connecting to Email: {e}."
            logger.error(response.error_message)
        finally:
            # Clean up only once per health check, no redundant logout+disconnect
            if created_here:
                self.disconnect()

        return response

    def native_query(self, query: str) -> StatusResponse:
        """Receive and process a raw SQL query."""
        ast = parse_sql(query)
        return self.query(ast)

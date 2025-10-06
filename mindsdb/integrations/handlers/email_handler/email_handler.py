# mindsdb/integrations/handlers/email_handler/email_handler.py

import email
import imaplib
import re
from email.header import decode_header

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.utilities import log

logger = log.getLogger(__name__)


# Suggestion 2: Re-introduce a dedicated table class
class EmailsTable(APITable):
    def __init__(self, handler):
        super().__init__(handler)

    def select(self, query) -> pd.DataFrame:
        parts = str(query).lower().split()
        mailbox = "inbox"
        # Restrict mailbox to a fixed allowlist to prevent traversal or unauthorized access
        allowed_mailboxes = {"inbox", "sent", "drafts", "spam", "trash"}
        if "where" in parts and "mailbox=" in str(query).lower():
            mailbox_part = str(query).lower().split("mailbox=")[1].strip()
            mailbox_candidate = mailbox_part.split()[0].strip("'\"").lower()
            if mailbox_candidate in allowed_mailboxes:
                mailbox = mailbox_candidate
            else:
                raise ValueError(f"Invalid mailbox name: '{mailbox_candidate}'. Allowed: {allowed_mailboxes}")
        else:
            mailbox = "inbox"

        conn = self.handler.connect()
        try:
            conn.select(mailbox)
            ...
            return pd.DataFrame(emails)
        finally:
            try:
                conn.logout()
            except Exception:
                pass

    def get_columns(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"Field": "id", "Type": "string"},
                {"Field": "from", "Type": "string"},
                {"Field": "subject", "Type": "string"},
                {"Field": "body", "Type": "string"},
                {"Field": "date", "Type": "string"},
            ]
        )


class EmailHandler(APIHandler):
    """
    This handler allows connecting to an email account via IMAP and querying emails.
    """

    def __init__(self, name: str, **kwargs):
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.is_connected = False
        self.connection = None

        self.email_address = self.connection_data.get("email")
        self.password = self.connection_data.get("password")

        if not self.email_address:
            raise ValueError("'email' parameter is required.")

        self.host = self.connection_data.get("host")
        self.port = self.connection_data.get("port")
        self.username = self.connection_data.get("username", self.email_address)
        self.use_ssl = self.connection_data.get("use_ssl", True)

        if not self.host:
            try:
                domain = self.email_address.split("@")[1]
                self.host = f"imap.{domain}"
                logger.info(
                    f"IMAP host not provided. Deduced '{self.host}' from email address."
                )
            except IndexError:
                raise ValueError("Invalid email address provided. Cannot deduce IMAP host.")

        if not self.port:
            self.port = 993

        # Suggestion 2: Register an instance of EmailsTable, not the handler itself
        self._register_table("emails", EmailsTable(self))

    def connect(self) -> imaplib.IMAP4:
        if not self.username or not self.password:
            raise ValueError("Username and password are required for IMAP connection.")

        if self.is_connected and self.connection:
            return self.connection
        try:
            # Suggestion 8: Logging is already safe (does not log password)
            logger.info(
                f"Connecting to {self.host}:{self.port} with user '{self.username}'..."
            )
            if self.use_ssl:
                self.connection = imaplib.IMAP4_SSL(self.host, self.port)
            else:
                self.connection = imaplib.IMAP4(self.host, self.port)
            status, _ = self.connection.login(self.username, self.password)
            if status != "OK":
                raise ConnectionError("IMAP login failed.")
            self.is_connected = True
            return self.connection
        except Exception as e:
            self.is_connected = False
            # Suggestion 8: Logging exception 'e' is also safe as it won't contain the password
            logger.error(f"Error connecting to email server: {e}")
            raise

    def disconnect(self):
        if self.is_connected and self.connection:
            self.connection.logout()
            self.is_connected = False
            self.connection = None

    def check_connection(self) -> HandlerStatusResponse:
        try:
            self.connect()
            self.disconnect()
            return HandlerStatusResponse(success=True)
        except Exception as e:
            return HandlerStatusResponse(success=False, error_message=str(e))
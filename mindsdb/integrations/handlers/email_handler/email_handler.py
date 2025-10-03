# mindsdb/integrations/handlers/email_handler/email_handler.py

import email
import imaplib
import re
from email.header import decode_header

import pandas as pd

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.utilities import log

logger = log.getLogger(__name__)


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

        # Suggestion 2: Check for missing email before using it
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

        self._register_table("emails", self)

    def connect(self) -> imaplib.IMAP4:
        # Suggestion 3: Check for missing credentials
        if not self.username or not self.password:
            raise ValueError("Username and password are required for IMAP connection.")

        if self.is_connected and self.connection:
            return self.connection
        try:
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

    def select(self, query) -> pd.DataFrame:
        parts = str(query).lower().split()
        mailbox = "inbox"
        if "where" in parts and "mailbox=" in str(query).lower():
            mailbox_part = str(query).lower().split("mailbox=")[1].strip()
            mailbox = mailbox_part.split()[0].strip("'\"")

        # Suggestion 7: Sanitize the mailbox name to prevent injection
        if not re.match(r"^[a-zA-Z0-9_./-]+$", mailbox):
            raise ValueError(
                f"Invalid mailbox name: '{mailbox}'. Only alphanumeric characters, underscore, dash, period, and slash are allowed."
            )

        conn = self.connect()
        conn.select(mailbox)

        status, messages = conn.search(None, "ALL")
        if status != "OK":
            raise Exception(f"Failed to search mailbox: {messages}")

        # Suggestion 4: Handle empty mailboxes gracefully
        if not messages or not messages[0]:
            return pd.DataFrame()

        email_ids = messages[0].split()
        limit = query.limit.value if query.limit else 25
        ids_to_fetch = email_ids[-limit:]

        if not ids_to_fetch:
            return pd.DataFrame()

        emails = []

        # Suggestion 5: Batch fetch emails for performance
        id_string = b",".join(ids_to_fetch)
        status, msg_data = conn.fetch(id_string, "(RFC822)")
        if status != "OK":
            raise Exception("Failed to fetch emails.")

        for response_part in msg_data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                subject, encoding = decode_header(msg["Subject"])[0]
                if isinstance(subject, bytes):
                    subject = subject.decode(encoding if encoding else "utf-8", "ignore")
                from_ = msg.get("From")
                body = ""
                if msg.is_multipart():
                    for part in msg.walk():
                        if part.get_content_type() == "text/plain":
                            try:
                                body = part.get_payload(decode=True).decode(
                                    errors="ignore"
                                )
                                break
                            except Exception:
                                pass
                else:
                    try:
                        body = msg.get_payload(decode=True).decode(errors="ignore")
                    except Exception:
                        pass
                emails.append(
                    {
                        # We extract the ID from the response part itself
                        "id": response_part[0].split()[0].decode(),
                        "from": from_,
                        "subject": subject,
                        "body": body,
                        "date": msg.get("Date"),
                    }
                )

        return pd.DataFrame(emails)

    def get_columns(self, table_name: str = "emails") -> pd.DataFrame:
        return pd.DataFrame(
            [
                {"Field": "id", "Type": "string"},
                {"Field": "from", "Type": "string"},
                {"Field": "subject", "Type": "string"},
                {"Field": "body", "Type": "string"},
                {"Field": "date", "Type": "string"},
            ]
        )
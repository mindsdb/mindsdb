import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict


class EmailSearchOptions(BaseModel):
    """
    Represents IMAP search options to use when searching emails
    """

    # IMAP mailbox to search.
    mailbox: str = "INBOX"
    # Search by email subject.
    subject: Optional[str] = None
    # Search based on who the email was sent to.
    to_field: Optional[str] = None
    # Search based on who the email was from.
    from_field: Optional[str] = None
    # Search based on when the email was received.
    since_date: Optional[datetime.date] = None
    until_date: Optional[datetime.date] = None
    # Search for all emails after this ID.
    since_email_id: Optional[int] = None

    # Pydantic v2 config (removes deprecation warnings)
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mailbox": "INBOX",
                "subject": "Test",
                "to_email": "example@example.com",
                "from_email": "hello@example.com",
                "since_date": "2021-01-01",
                "until_date": "2021-01-31",
                "since_email_id": "123",
            }
        },
        extra="forbid",
    )


class EmailConnectionDetails(BaseModel):
    """
    Represents the connection details for an email client.
    Backward compatible with previous fields and extended with advanced options.
    """

    email: str
    password: str

    # Backward-compat legacy fields (kept for compatibility)
    imap_server: str = "imap.gmail.com"
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587

    # Advanced IMAP fields
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_use_ssl: bool = True
    imap_use_starttls: bool = False
    imap_username: Optional[str] = None

    # Advanced SMTP fields
    smtp_host: Optional[str] = None
    smtp_starttls: bool = True
    smtp_username: Optional[str] = None  # not used currently but left for parity

    # Pydantic v2 config (removes deprecation warnings)
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "email": "joe@bloggs.com",
                "password": "password",
                "imap_host": "127.0.0.1",
                "imap_port": 1143,
                "imap_use_ssl": False,
                "imap_use_starttls": True,
                "imap_username": "joe@localhost",
                "smtp_host": "127.0.0.1",
                "smtp_port": 587,
                "smtp_starttls": True,
            }
        },
        extra="forbid",
    )

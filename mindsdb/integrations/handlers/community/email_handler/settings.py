import datetime
from pydantic import BaseModel


class EmailSearchOptions(BaseModel):
    """
    Represents IMAP search options to use when searching emails
    """
    # IMAP mailbox to search.
    mailbox: str = "INBOX"
    # Search by email subject.
    subject: str = None
    # Search based on who the email was sent to.
    to_field: str = None
    # Search based on who the email was from.
    from_field: str = None
    # Search based on when the email was received.
    since_date: datetime.date = None
    until_date: datetime.date = None
    # Search for all emails after this ID.
    since_email_id: int = None

    class Config:
        json_schema_extra = {
            "example": {
                "mailbox": "INBOX",
                "subject": "Test",
                "to_email": "example@example.com",
                "from_email": "hello@example.com",
                "since_date": "2021-01-01",
                "until_date": "2021-01-31",
                "since_email_id": "123"
            }

        }
        extra = "forbid"


class EmailConnectionDetails(BaseModel):
    """
    Represents the connection details for an email client
    """
    email: str
    password: str
    imap_server: str = "imap.gmail.com"
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587

    class Config:
        json_schema_extra = {
            "example": {
                "email": "joe@bloggs.com",
                "password": "password",
                "imap_server": "imap.gmail.com",
                "smtp_server": "smtp.gmail.com",
                "smtp_port": 587
            }
        }
        extra = "forbid"

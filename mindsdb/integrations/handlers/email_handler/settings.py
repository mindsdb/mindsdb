import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


class EmailSearchOptions(BaseModel):
    """
    Represents IMAP search options to use when searching emails.
    """

    mailbox: str = "INBOX"
    subject: Optional[str] = None
    to_field: Optional[str] = None
    from_field: Optional[str] = None
    since_date: Optional[datetime.date] = None
    until_date: Optional[datetime.date] = None
    # Search for all emails after this ID (UID, integer in practice).
    since_email_id: Optional[int] = None
    # Cap total messages fetched (client-side). Prevents excessive IMAP calls.
    # Default None -> use client default cap; enforce sensible bounds if explicitly set.
    max_results: Optional[int] = Field(default=None, ge=1, le=5000)

    # Ensure mailbox is not blank; default back to INBOX if an empty string is provided
    @field_validator("mailbox")
    @classmethod
    def _validate_mailbox_non_empty(cls, v: str) -> str:
        v = (v or "").strip()
        return v or "INBOX"

    # Enforce since_date <= until_date when both provided
    @field_validator("until_date")
    @classmethod
    def _validate_date_range(cls, v, info):
        since_date = info.data.get("since_date")
        if v is not None and since_date is not None and since_date > v:
            raise ValueError("until_date must be on or after since_date")
        return v

    # Normalize optional string filters to stripped values (no-op for None)
    @field_validator("subject", "to_field", "from_field")
    @classmethod
    def _strip_optional_strings(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if isinstance(v, str) else v

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "mailbox": "INBOX",
                "subject": "Test",
                "to_field": "example@example.com",
                "from_field": "hello@example.com",
                "since_date": "2021-01-01",
                "until_date": "2021-01-31",
                "since_email_id": 123,
                "max_results": 250,
            }
        },
        extra="forbid",
    )


class EmailConnectionDetails(BaseModel):
    """
    Represents the connection details for an email client.
    Backward compatible with legacy fields and extended with advanced options.
    """

    email: str
    password: str

    # Legacy defaults (kept for compatibility)
    imap_server: str = "imap.gmail.com"
    smtp_server: str = "smtp.gmail.com"
    smtp_port: int = 587

    # Advanced IMAP fields
    imap_host: Optional[str] = None
    imap_port: Optional[int] = None
    imap_use_ssl: bool = True
    imap_use_starttls: bool = False
    imap_username: Optional[str] = None
    imap_timeout: Optional[float] = 30.0  # seconds

    # Advanced SMTP fields
    smtp_host: Optional[str] = None
    smtp_starttls: bool = True
    smtp_username: Optional[str] = None  # use if SMTP auth differs from email
    smtp_timeout: Optional[float] = 30.0  # seconds

    # Enforce non-empty credentials
    @field_validator("email", "password")
    @classmethod
    def _non_empty_credentials(cls, v: str) -> str:
        v = (v or "").strip()
        if not v:
            raise ValueError("must be a non-empty string")
        return v

    # Normalize optional username values (strip whitespace)
    @field_validator("imap_username", "smtp_username", "imap_host", "smtp_host")
    @classmethod
    def _strip_optional_strings(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if isinstance(v, str) else v

    # Validate ports if provided (must be positive)
    @field_validator("imap_port", "smtp_port")
    @classmethod
    def _validate_ports_positive(cls, v: Optional[int]) -> Optional[int]:
        if v is not None and v <= 0:
            raise ValueError("port must be a positive integer")
        return v

    # Validate timeouts if provided (must be positive)
    @field_validator("imap_timeout", "smtp_timeout")
    @classmethod
    def _validate_timeouts_positive(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v <= 0:
            raise ValueError("timeout must be a positive value (seconds)")
        return v

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
                "imap_timeout": 30.0,
                "smtp_host": "127.0.0.1",
                "smtp_port": 587,
                "smtp_starttls": True,
                "smtp_username": "joe@localhost",
                "smtp_timeout": 30.0,
            }
        },
        extra="forbid",
    )

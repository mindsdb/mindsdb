from __future__ import annotations

import email
import imaplib
import smtplib
import unicodedata
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape
from typing import Iterable, List, Optional, Tuple

from datetime import datetime, timedelta

import pandas as pd
from mindsdb.integrations.handlers.email_handler.settings import (
    EmailSearchOptions,
    EmailConnectionDetails,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def _decode_mime_header(value: Optional[str]) -> str:
    if not value:
        return ""
    try:
        parts = decode_header(value)
        decoded = []
        for part, enc in parts:
            if isinstance(part, bytes):
                try:
                    decoded.append(part.decode(enc or "utf-8", errors="ignore"))
                except Exception:
                    decoded.append(part.decode("utf-8", errors="ignore"))
            else:
                decoded.append(part)
        return "".join(decoded)
    except Exception:
        return str(value)


def _chunked(iterable: List[bytes], size: int) -> Iterable[List[bytes]]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _remove_bidi_and_controls(text: str) -> str:
    """Remove Unicode bidirectional override and general control/format characters."""
    cleaned_chars = []
    for ch in text:
        cat = unicodedata.category(ch)  # e.g., 'Cf', 'Cc'
        # Reject any 'Other' categories (all 'C*') to avoid controls, formats, surrogates, etc.
        if cat.startswith("C"):
            continue
        cleaned_chars.append(ch)
    return "".join(cleaned_chars)


def _sanitize_for_ui(text: str) -> str:
    """
    Normalize to NFKC, strip bidi/control chars, then HTML-escape for safe rendering.
    Keeps the original 'body' separate for programmatic use.
    """
    try:
        normalized = unicodedata.normalize("NFKC", text)
    except Exception:
        normalized = text
    without_controls = _remove_bidi_and_controls(normalized)
    return html_escape(without_controls)


def _sanitize_mailbox(mailbox: str) -> str:
    """
    Allow typical IMAP mailbox names including hierarchies (e.g., 'INBOX', 'INBOX.Sent', 'Sent Mail').
    Disallow traversal or path-like injection: '/', '\\', null bytes, '..' segments, and any Unicode control/format chars.
    """
    mb = (mailbox or "INBOX").strip()

    # Normalize to reduce homoglyph tricks; mail servers typically accept normalized names.
    try:
        mb = unicodedata.normalize("NFKC", mb)
    except Exception:
        pass

    # Reject obvious path injection
    if any(bad in mb for bad in ("/", "\\", "\x00")):
        raise ValueError("Invalid mailbox name: disallowed characters detected.")

    # Reject traversal segments
    if ".." in mb:
        raise ValueError("Invalid mailbox name: traversal patterns are not allowed.")

    # Reject non-printable / control-like characters (all Unicode 'C*' categories)
    for ch in mb:
        if unicodedata.category(ch).startswith("C"):
            raise ValueError("Invalid mailbox name: contains control/unassigned characters.")

    # Bounds
    if len(mb) > 255:
        raise ValueError("Invalid mailbox name: too long.")

    return mb


class EmailClient:
    """IMAP/SMTP client with safe defaults and advanced options support."""

    # Reasonable defaults
    _DEFAULT_SINCE_DAYS = 10
    _UID_FETCH_CHUNK = 50  # batch size for UID FETCH
    _MAX_RESULTS_DEFAULT = 500  # cap to prevent excessive sequential IMAP calls

    def __init__(self, connection_data: EmailConnectionDetails):
        # Credentials
        self.email = connection_data.email
        self.password = connection_data.password

        # IMAP settings with backward-compatible defaults
        self.imap_username = connection_data.imap_username or self.email
        imap_host = connection_data.imap_host or connection_data.imap_server
        imap_port = (
            connection_data.imap_port
            if connection_data.imap_port is not None
            else (993 if connection_data.imap_use_ssl else 143)
        )

        if connection_data.imap_use_ssl:
            self.imap_server = imaplib.IMAP4_SSL(imap_host, imap_port)
            self._imap_starttls = False
        else:
            self.imap_server = imaplib.IMAP4(imap_host, imap_port)
            self._imap_starttls = bool(connection_data.imap_use_starttls)

        # SMTP settings with backward-compatible defaults
        smtp_host = connection_data.smtp_host or connection_data.smtp_server
        smtp_port = connection_data.smtp_port
        self.smtp_server = smtplib.SMTP(smtp_host, smtp_port)
        self._smtp_starttls = bool(connection_data.smtp_starttls)

    def _ensure_imap_session(self) -> None:
        """Login and optionally STARTTLS for IMAP if needed."""
        if self._imap_starttls:
            try:
                self.imap_server.starttls()
            except Exception as e:
                raise ValueError(f"IMAP STARTTLS failed: {e}")
        ok, resp = self.imap_server.login(self.imap_username, self.password)
        if ok != "OK":
            raise ValueError(f"Unable to login to IMAP: {resp}")

    def select_mailbox(self, mailbox: str = "INBOX") -> None:
        """Login & select a mailbox from IMAP server (mailbox is sanitized internally)."""
        target_mailbox = _sanitize_mailbox(mailbox)
        self._ensure_imap_session()
        ok, resp = self.imap_server.select(target_mailbox)
        if ok != "OK":
            raise ValueError(f"Unable to select mailbox {target_mailbox}. Please check the mailbox name: {resp}")
        logger.info(f"Selected mailbox {target_mailbox}")

    def logout(self) -> None:
        """Shuts down the connection to the IMAP and SMTP server."""
        # IMAP
        try:
            try:
                ok, resp = self.imap_server.logout()
                if ok not in ("BYE", "OK"):
                    logger.error(f"Unable to logout of IMAP client: {str(resp)}")
            except Exception as e:
                logger.error(f"Exception occurred while logging out from IMAP server: {str(e)}")
        finally:
            pass

        # SMTP
        try:
            self.smtp_server.quit()
        except Exception as e:
            logger.error(f"Exception occurred while logging out of SMTP server: {str(e)}")

    def send_email(self, to_addr: str, subject: str, body: str) -> None:
        """Send an email."""
        msg = MIMEMultipart()
        msg["From"] = self.email
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        if self._smtp_starttls:
            self.smtp_server.starttls()
        self.smtp_server.login(self.email, self.password)
        self.smtp_server.send_message(msg)
        logger.info(f"Email sent to {to_addr} with subject: {subject}")

    def _build_search_query(self, options: EmailSearchOptions) -> str:
        query_parts: List[str] = []

        if options.subject:
            query_parts.append(f'(SUBJECT "{options.subject}")')

        if options.to_field:
            query_parts.append(f'(TO "{options.to_field}")')

        if options.from_field:
            query_parts.append(f'(FROM "{options.from_field}")')

        if options.since_date is not None:
            since_date_str = options.since_date.strftime("%d-%b-%Y")
        else:
            since_date = datetime.today() - timedelta(days=EmailClient._DEFAULT_SINCE_DAYS)
            since_date_str = since_date.strftime("%d-%b-%Y")
        query_parts.append(f'(SINCE "{since_date_str}")')

        if options.until_date is not None:
            until_date_str = options.until_date.strftime("%d-%b-%Y")
            query_parts.append(f'(BEFORE "{until_date_str}")')

        if options.since_email_id is not None:
            query_parts.append(f"(UID {options.since_email_id}:*)")

        if not query_parts:
            return "ALL"
        return " ".join(query_parts)

    def _fetch_messages_by_uids(self, uids: List[bytes]) -> List[Tuple[bytes, bytes]]:
        """
        Fetch messages in chunks using comma-separated UID lists.
        Returns a flat list of (meta, raw_message) pairs (only the tuple entries).
        """
        results: List[Tuple[bytes, bytes]] = []
        if not uids:
            return results

        for chunk in _chunked(uids, EmailClient._UID_FETCH_CHUNK):
            # UID expects comma-separated string of ids
            id_string = ",".join(uid.decode("ascii", errors="ignore") for uid in chunk)
            status, data = self.imap_server.uid("fetch", id_string, "(RFC822)")
            if status != "OK":
                raise RuntimeError("Failed to fetch emails via IMAP UID FETCH.")
            for part in data:
                if isinstance(part, tuple) and len(part) >= 2:
                    results.append((part[0], part[1]))
        return results

    def search_email(self, options: EmailSearchOptions) -> pd.DataFrame:
        """
        Search emails based on the given options and return a DataFrame.
        Uses UID search/fetch and chunked fetching for performance and correctness.
        Applies an upper bound on total fetched messages to avoid excessive IMAP calls.
        """
        self.select_mailbox(options.mailbox)

        try:
            query = self._build_search_query(options)
            status, items = self.imap_server.uid("search", None, query)
            if status != "OK":
                raise RuntimeError("IMAP UID SEARCH failed.")
            raw_list = items[0].split() if items and items[0] else []
            if not raw_list:
                return pd.DataFrame([])

            # Apply server-side limit hint (client-side capping) to reduce total UID FETCH calls
            max_results = options.max_results or EmailClient._MAX_RESULTS_DEFAULT
            if max_results is not None and max_results > 0:
                # Take the most recent UIDs (at the end); IMAP servers return ascending by default.
                raw_list = raw_list[-max_results:]

            fetched = self._fetch_messages_by_uids(raw_list)

            ret = []
            for meta, raw in fetched:
                try:
                    msg = email.message_from_bytes(raw)
                except Exception:
                    # Skip undecodable messages instead of failing the batch
                    continue

                subject = _decode_mime_header(msg.get("Subject"))
                from_addr = _decode_mime_header(msg.get("From"))
                date_hdr = _decode_mime_header(msg.get("Date"))

                plain_payload = None
                html_payload = None
                content_type = "html"
                if msg.is_multipart():
                    for part in msg.walk():
                        subtype = part.get_content_subtype()
                        if subtype == "plain" and part.get_content_type() == "text/plain":
                            try:
                                plain_payload = part.get_payload(decode=True)
                                content_type = "plain"
                                # Prioritize plain text and break early
                                break
                            except Exception:
                                plain_payload = None
                        elif subtype == "html" and part.get_content_type() == "text/html":
                            try:
                                html_payload = part.get_payload(decode=True)
                            except Exception:
                                html_payload = None
                else:
                    # Non-multipart: attempt to decode as text/plain first
                    ctype = msg.get_content_type()
                    try:
                        payload = msg.get_payload(decode=True)
                    except Exception:
                        payload = None
                    if ctype == "text/plain":
                        plain_payload = payload
                        content_type = "plain"
                    elif ctype == "text/html":
                        html_payload = payload
                        content_type = "html"

                body_bytes = plain_payload or html_payload
                if body_bytes is None:
                    # Skip messages without useful bodies
                    continue

                try:
                    body_text = body_bytes.decode("utf-8", errors="ignore")
                except Exception:
                    body_text = ""

                # Security: provide an HTML-escaped, normalized, control-free variant safe for UI rendering
                body_safe = _sanitize_for_ui(body_text)

                # Attempt to pull UID from meta tuple
                email_id = None
                try:
                    if isinstance(meta, (bytes, bytearray)):
                        # meta format often like: b'1 (UID 12345 RFC822 {bytes})'
                        parts = meta.decode("utf-8", errors="ignore").split()
                        if "UID" in parts:
                            uid_idx = parts.index("UID") + 1
                            email_id = parts[uid_idx]
                except Exception:
                    email_id = None

                ret.append(
                    {
                        "id": email_id,
                        "to_field": _decode_mime_header(msg.get("To")),
                        "from_field": from_addr,
                        "subject": subject,
                        "date": date_hdr,
                        "body": body_text,
                        "body_safe": body_safe,  # safe for UI rendering (normalized + escaped, no bidi/control chars)
                        "body_content_type": content_type,
                    }
                )

            return pd.DataFrame(ret)
        except Exception as e:
            raise Exception("Error searching email") from e

from __future__ import annotations

import email
import imaplib
import smtplib
import ssl
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from html import escape as html_escape
from typing import Iterable, List, Optional, Tuple

import chardet
import pandas as pd
from mindsdb.integrations.handlers.email_handler.settings import (
    EmailConnectionDetails,
    EmailSearchOptions,
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
                    decoded.append(part.decode(enc or "utf-8", errors="replace"))
                except Exception:
                    decoded.append(part.decode("utf-8", errors="replace"))
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
    Strict mailbox validation:
    - Normalize to NFKC
    - Allow '/' as a hierarchy delimiter; forbid leading '/' (absolute paths)
    - Forbid path traversal ('.' or '..' segments anywhere) and empty segments
    - Forbid nulls and Unicode control/format characters
    - Only allow ASCII letters/digits and the safe set: " .-_&@'()[]"
    - Max length: 255
    """
    mb = (mailbox or "INBOX").strip()

    try:
        mb = unicodedata.normalize("NFKC", mb)
    except Exception:
        pass

    if "\x00" in mb or mb.startswith("/"):
        raise ValueError("Invalid mailbox name.")

    segments = mb.split("/")
    if any(s in ("", ".", "..") for s in segments):
        raise ValueError("Invalid mailbox path segments.")

    for seg in segments:
        for ch in seg:
            if not ch.isascii():
                raise ValueError("Invalid mailbox name: non-ASCII characters are not allowed.")
            if unicodedata.category(ch).startswith("C"):
                raise ValueError("Invalid mailbox name: control characters not allowed.")
            if not (ch.isalnum() or ch in " .-_&@'()[]"):
                raise ValueError("Invalid mailbox name: contains unsupported characters.")

    if len(mb) > 255:
        raise ValueError("Invalid mailbox name: too long.")

    return "/".join(segments)


def _imap_quote(value: str) -> str:
    """
    IMAP-safe quoting for SEARCH values.
    - Normalize to NFKC
    - Strip CR/LF
    - Reject any non-ASCII or control characters (ord < 32 or ord > 126)
    - Escape backslashes and double quotes
    """
    v = unicodedata.normalize("NFKC", value or "")
    v = v.replace("\r", "").replace("\n", "")
    for ch in v:
        o = ord(ch)
        if o < 32 or o > 126:
            raise ValueError("Invalid IMAP search value: only ASCII printable characters are allowed.")
    v = v.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{v}"'


def _decode_bytes(payload: Optional[bytes], declared_charset: Optional[str]) -> str:
    """
    Decode email payload bytes using declared charset, falling back to chardet guess, then UTF-8 replace.
    """
    if not payload:
        return ""
    if declared_charset:
        try:
            return payload.decode(declared_charset, errors="replace")
        except Exception:
            pass
    try:
        det = chardet.detect(payload)
        enc = det.get("encoding")
        if enc:
            return payload.decode(enc, errors="replace")
    except Exception:
        pass
    return payload.decode("utf-8", errors="replace")


class EmailClient:
    """IMAP/SMTP client with safe defaults and advanced options support."""

    _DEFAULT_SINCE_DAYS = 10
    _UID_FETCH_CHUNK = 50
    _FETCH_ALL_THRESHOLD = 500
    _MAX_RESULTS_DEFAULT = 500

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
        self.imap_timeout = connection_data.imap_timeout or 30.0

        if connection_data.imap_use_ssl:
            self.imap_server = imaplib.IMAP4_SSL(
                imap_host, imap_port, ssl_context=ssl.create_default_context(), timeout=self.imap_timeout
            )
            self._imap_starttls = False
        else:
            self.imap_server = imaplib.IMAP4(imap_host, imap_port, timeout=self.imap_timeout)
            self._imap_starttls = bool(connection_data.imap_use_starttls)

        # Connection state
        self._imap_authenticated = False

        # SMTP settings with backward-compatible defaults
        smtp_host = connection_data.smtp_host or connection_data.smtp_server
        smtp_port = connection_data.smtp_port
        self.smtp_timeout = connection_data.smtp_timeout or 30.0
        self.smtp_server = smtplib.SMTP(smtp_host, smtp_port, timeout=self.smtp_timeout)
        self._smtp_starttls = bool(connection_data.smtp_starttls)
        self.smtp_username = connection_data.smtp_username or self.email
        self._smtp_host = smtp_host
        self._smtp_port = smtp_port

        self._smtp_starttls = bool(connection_data.smtp_starttls)
        # Allow SMTP username override; fallback to 'email'
        self.smtp_username = connection_data.smtp_username or self.email

    def _ensure_imap_session(self) -> None:
        """Login and optionally STARTTLS for IMAP if needed (idempotent)."""
        if self._imap_authenticated:
            return
        if self._imap_starttls:
            try:
                ctx = ssl.create_default_context()
                # starttls signature is starttls(ssl_context=...) on Python 3.12+
                self.imap_server.starttls(ssl_context=ctx)
            except Exception as e:
                raise ValueError(f"IMAP STARTTLS failed: {e}")
        ok, resp = self.imap_server.login(self.imap_username, self.password)
        if ok != "OK":
            raise ValueError(f"Unable to login to IMAP: {resp}")
        self._imap_authenticated = True

    def select_mailbox(self, mailbox: str = "INBOX") -> None:
        """Login & select a mailbox from IMAP server (mailbox is sanitized internally)."""
        target_mailbox = _sanitize_mailbox(mailbox)
        self._ensure_imap_session()
        ok, resp = self.imap_server.select(target_mailbox)
        if ok != "OK":
            raise ValueError(f"Unable to select mailbox {target_mailbox}. Please check the mailbox name: {resp}")
        logger.debug(f"Selected mailbox {target_mailbox}")

    def logout(self) -> None:
        """Shuts down the connection to the IMAP and SMTP server."""
        # IMAP
        try:
            if hasattr(self, "imap_server") and self.imap_server is not None:
                try:
                    ok, resp = self.imap_server.logout()
                    if ok not in ("BYE", "OK"):
                        logger.error(f"Unable to logout of IMAP client: {str(resp)}")
                except Exception as e:
                    logger.error(f"Exception occurred while logging out from IMAP server: {str(e)}")
        finally:
            self._imap_authenticated = False
            # Prevent use-after-close
            try:
                self.imap_server = None
            except Exception:
                pass

        # SMTP
        try:
            if hasattr(self, "smtp_server") and self.smtp_server is not None:
                self.smtp_server.quit()
        except Exception as e:
            logger.error(f"Exception occurred while logging out of SMTP server: {str(e)}")
        finally:
            # Prevent use-after-close
            try:
                self.smtp_server = None
            except Exception:
                pass

    def send_email(self, to_addr: str, subject: str, body: str) -> None:
        """Send an email."""
        msg = MIMEMultipart()
        msg["From"] = self.email
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        try:
            if self._smtp_starttls:
                ctx = ssl.create_default_context()
                self.smtp_server.ehlo()
                self.smtp_server.starttls(context=ctx)
                self.smtp_server.ehlo()
            self.smtp_server.login(self.smtp_username, self.password)
            self.smtp_server.send_message(msg)
            logger.info(f"Email sent to {to_addr} with subject: {subject}")
        except Exception as e:
            # Do not log credentials
            logger.error(f"Failed sending email to {to_addr}: {e}")
            # Ensure SMTP connection is closed on error to avoid leaks and leave a clean state
            try:
                if self.smtp_server is not None:
                    self.smtp_server.quit()
            except Exception:
                pass
            finally:
                try:
                    self.smtp_server = None
                except Exception:
                    pass
            raise

    def _build_search_query(self, options: EmailSearchOptions) -> str:
        query_parts: List[str] = []

        if options.subject:
            query_parts.append(f"(SUBJECT {_imap_quote(options.subject)})")

        if options.to_field:
            query_parts.append(f"(TO {_imap_quote(options.to_field)})")

        if options.from_field:
            query_parts.append(f"(FROM {_imap_quote(options.from_field)})")

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

    @staticmethod
    def _uid_bytes_to_str(uid: bytes) -> Optional[str]:
        """
        Safely convert UID bytes to a string for IMAP commands.
        IMAP UIDs are decimal digits; we default to ASCII, with a UTF-8 fallback and validation.
        """
        for enc in ("ascii", "utf-8"):
            try:
                s = uid.decode(enc, errors="strict")
                if s.isdigit():
                    return s
            except Exception:
                continue
        logger.warning("Skipping non-numeric UID returned by server.")
        return None

    def _fetch_messages_by_uids(self, uids: List[bytes]) -> List[Tuple[bytes, bytes]]:
        """
        Fetch messages using comma-separated UID lists:
        - If total UIDs <= _FETCH_ALL_THRESHOLD: single fetch to reduce round-trips.
        - Else: chunked fetches with _UID_FETCH_CHUNK.
        Returns a flat list of (meta, raw_message) pairs.
        """
        results: List[Tuple[bytes, bytes]] = []
        if not uids:
            return results

        # Convert and filter UIDs robustly
        uid_strings: List[str] = []
        for uid in uids:
            s = self._uid_bytes_to_str(uid)
            if s is not None:
                uid_strings.append(s)

        if not uid_strings:
            return results

        def _fetch(id_string: str) -> None:
            status, data = self.imap_server.uid("fetch", id_string, "(UID RFC822)")
            if status != "OK":
                raise RuntimeError("Failed to fetch emails via IMAP UID FETCH.")
            for part in data:
                if isinstance(part, tuple) and len(part) >= 2:
                    results.append((part[0], part[1]))

        if len(uid_strings) <= EmailClient._FETCH_ALL_THRESHOLD:
            _fetch(",".join(uid_strings))
            return results

        for i in range(0, len(uid_strings), EmailClient._UID_FETCH_CHUNK):
            _fetch(",".join(uid_strings[i : i + EmailClient._UID_FETCH_CHUNK]))

        return results

    def search_email(self, options: EmailSearchOptions) -> pd.DataFrame:
        """
        Search emails based on the given options and return a DataFrame.
        Uses UID search/fetch and efficient batching. Applies an upper bound on
        total fetched messages to avoid excessive IMAP calls.
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

            # Apply cap to reduce total fetch volume; prefer the most recent UIDs
            max_results = options.max_results or EmailClient._MAX_RESULTS_DEFAULT
            if max_results is not None and max_results > 0:
                raw_list = raw_list[-max_results:]

            fetched = self._fetch_messages_by_uids(raw_list)

            def _parse_one(meta_raw: Tuple[bytes, bytes]) -> Optional[dict]:
                meta, raw = meta_raw
                try:
                    msg = email.message_from_bytes(raw)
                except Exception:
                    return None

                subject = _decode_mime_header(msg.get("Subject"))
                from_addr = _decode_mime_header(msg.get("From"))
                date_hdr = _decode_mime_header(msg.get("Date"))

                plain_payload = None
                html_payload = None
                content_type = "html"
                declared_charset = None

                if msg.is_multipart():
                    for part in msg.walk():
                        subtype = part.get_content_subtype()
                        if subtype == "plain" and part.get_content_type() == "text/plain":
                            try:
                                plain_payload = part.get_payload(decode=True)
                                declared_charset = part.get_content_charset() or msg.get_content_charset()
                                content_type = "plain"
                                break
                            except Exception:
                                plain_payload = None
                        elif subtype == "html" and part.get_content_type() == "text/html":
                            try:
                                html_payload = part.get_payload(decode=True)
                                declared_charset = part.get_content_charset() or msg.get_content_charset()
                            except Exception:
                                html_payload = None
                else:
                    ctype = msg.get_content_type()
                    try:
                        payload = msg.get_payload(decode=True)
                    except Exception:
                        payload = None
                    declared_charset = msg.get_content_charset()
                    if ctype == "text/plain":
                        plain_payload = payload
                        content_type = "plain"
                    elif ctype == "text/html":
                        html_payload = payload
                        content_type = "html"

                body_bytes = plain_payload or html_payload
                if body_bytes is None:
                    return None

                body_text = _decode_bytes(body_bytes, declared_charset)
                body_safe = _sanitize_for_ui(body_text)

                # Extract UID robustly
                email_id = None
                try:
                    if isinstance(meta, (bytes, bytearray)):
                        parts = meta.decode("utf-8", errors="replace").split()
                        if "UID" in parts:
                            uid_idx = parts.index("UID") + 1
                            if uid_idx < len(parts):
                                email_id = parts[uid_idx]
                except Exception:
                    email_id = None

                return {
                    "id": email_id,
                    "to_field": _decode_mime_header(msg.get("To")),
                    "from_field": from_addr,
                    "subject": subject,
                    "datetime": date_hdr,
                    "body_safe": body_safe,
                    "body_content_type": content_type,
                }

            # Parallelize message parsing/decoding for better throughput
            rows: List[dict] = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                for row in executor.map(_parse_one, fetched):
                    if row is not None:
                        rows.append(row)

            return pd.DataFrame(rows)
        except Exception as e:
            raise Exception("Error searching email") from e

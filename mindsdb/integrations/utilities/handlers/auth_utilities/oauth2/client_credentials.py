"""OAuth2 client credentials grant token provider.

A reusable, thread-safe utility that fetches and caches OAuth2 access tokens
using the RFC 6749 client credentials grant. Suitable for server-to-server
flows with no end-user redirect.
"""

from __future__ import annotations

import base64
import ipaddress
import socket
import threading
import time
from typing import Any, Optional, Union
from urllib.parse import urlparse

import requests

from mindsdb.utilities import log

logger = log.getLogger(__name__)


_ALLOWED_AUTH_METHODS = ("client_secret_post", "client_secret_basic")
_CONNECT_TIMEOUT_SECONDS = 10
_READ_TIMEOUT_SECONDS = 30
_DEFAULT_EXPIRES_IN_SECONDS = 300
_SKEW_SECONDS = 60
_MAX_RESPONSE_BYTES = 64 * 1024


def _is_localhost_name(host: str) -> bool:
    h = host.lower().rstrip(".")
    if h == "localhost":
        return True
    if h.endswith(".localhost"):
        return True
    if h in ("ip6-localhost", "ip6-loopback"):
        return True
    return False


def _ip_in_forbidden_range(ip: ipaddress._BaseAddress) -> Optional[str]:
    if ip.is_loopback:
        return "loopback"
    if ip.is_link_local:
        return "link-local"
    if ip.is_private:
        return "private"
    if ip.is_multicast:
        return "multicast"
    if ip.is_reserved:
        return "reserved"
    if ip.is_unspecified:
        return "unspecified"
    return None


def _validate_token_url(token_url: str) -> None:
    """Raise ValueError if token_url violates SSRF safety rules."""
    if not isinstance(token_url, str) or not token_url:
        raise ValueError("token_url must be a non-empty string")

    parsed = urlparse(token_url)
    scheme = parsed.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError(f"token_url scheme '{parsed.scheme}' is not allowed; only http and https are permitted")

    host = parsed.hostname
    if not host:
        raise ValueError("token_url must include a host component")

    if _is_localhost_name(host):
        raise ValueError(f"token_url host '{host}' is a localhost alias and is not permitted")

    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None

    if ip is not None:
        reason = _ip_in_forbidden_range(ip)
        if reason is not None:
            raise ValueError(f"token_url host '{host}' resolves to a forbidden range ({reason})")
    else:
        try:
            addrinfo = socket.getaddrinfo(host, None)
        except socket.gaierror as exc:
            raise ValueError(f"token_url host '{host}' could not be resolved: {exc}") from exc

        for info in addrinfo:
            addr = info[4][0]
            # Strip IPv6 zone identifier if present
            if "%" in addr:
                addr = addr.split("%", 1)[0]
            try:
                resolved_ip = ipaddress.ip_address(addr)
            except ValueError:
                continue
            reason = _ip_in_forbidden_range(resolved_ip)
            if reason is not None:
                raise ValueError(f"token_url host '{host}' resolves to a forbidden range ({reason})")

    if scheme == "http":
        logger.warning(
            "OAuth2 token_url uses http://; credentials will be transmitted over an unencrypted channel. host=%s",
            host,
        )


class OAuth2ClientCredentialsProvider:
    """Fetches and caches OAuth2 access tokens using the client credentials grant.

    Thread-safe: concurrent callers of get_token() during refresh trigger
    exactly one HTTP request to the token endpoint via double-checked locking.
    """

    def __init__(
        self,
        token_url: str,
        client_id: str,
        client_secret: str,
        scope: Union[str, list, None] = None,
        audience: Optional[str] = None,
        token_auth_method: str = "client_secret_post",
        handler_storage: Any = None,
        storage_key: str = "oauth_client_credentials_tokens",
    ) -> None:
        if token_auth_method not in _ALLOWED_AUTH_METHODS:
            raise ValueError(
                f"token_auth_method '{token_auth_method}' is not supported; "
                f"allowed values are: {', '.join(_ALLOWED_AUTH_METHODS)}"
            )

        _validate_token_url(token_url)

        self.token_url = token_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.audience = audience
        self.token_auth_method = token_auth_method
        self.handler_storage = handler_storage
        self.storage_key = storage_key

        self._lock = threading.Lock()
        self._memory_cache: Optional[dict] = None
        self._missing_expires_in_logged = False

    def get_token(self) -> str:
        """Return a valid access token, refreshing if needed."""
        cached = self._read_cache()
        if cached and not self._is_expired(cached):
            return cached["access_token"]

        with self._lock:
            # Re-read inside the lock — another thread may have refreshed while
            # we were waiting on the lock. Without this second check, two
            # threads that both observed an expired token would both refresh.
            cached = self._read_cache()
            if cached and not self._is_expired(cached):
                return cached["access_token"]

            new_token = self._request_token()
            self._write_cache(new_token)
            return new_token["access_token"]

    def invalidate(self) -> None:
        """Clear the cached token from both in-memory and persistent storage."""
        with self._lock:
            self._memory_cache = None
            if self.handler_storage is not None:
                try:
                    self.handler_storage.encrypted_json_set(self.storage_key, None)
                except Exception as exc:
                    logger.debug(
                        "Failed to clear OAuth2 token from handler_storage; cleared in-memory only. host=%s err=%s",
                        self._safe_host(),
                        exc,
                    )

    def current_secrets(self) -> list:
        """Return secrets that response-scrub layers should redact.

        Safe to call per-request. For the client credentials flow this is
        currently the cached access token if any.
        """
        cached = self._read_cache()
        if cached and not self._is_expired(cached):
            token = cached.get("access_token")
            if token:
                return [token]
        return []

    def _request_token(self) -> dict:
        body: dict = {"grant_type": "client_credentials"}

        if self.scope is not None:
            if isinstance(self.scope, (list, tuple)):
                scope_value = " ".join(str(s) for s in self.scope)
            else:
                scope_value = str(self.scope)
            if scope_value:
                body["scope"] = scope_value

        if self.audience is not None:
            # `audience` is an Auth0-style extension; also accepted by Cognito
            # and others. Not part of RFC 6749.
            body["audience"] = self.audience

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }

        if self.token_auth_method == "client_secret_post":
            body["client_id"] = self.client_id
            body["client_secret"] = self.client_secret
        else:
            credentials = f"{self.client_id}:{self.client_secret}".encode("utf-8")
            headers["Authorization"] = "Basic " + base64.b64encode(credentials).decode("ascii")

        try:
            response = requests.post(
                self.token_url,
                data=body,
                headers=headers,
                timeout=(_CONNECT_TIMEOUT_SECONDS, _READ_TIMEOUT_SECONDS),
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as exc:
            # Avoid leaking request body which contains credentials
            raise RuntimeError(
                f"OAuth2 token request failed (transport error). host={self._safe_host()} client_id={self.client_id}"
            ) from self._sanitize_exception(exc)

        try:
            return self._parse_token_response(response)
        finally:
            try:
                response.close()
            except Exception:
                pass

    def _parse_token_response(self, response: requests.Response) -> dict:
        if response.is_redirect or 300 <= response.status_code < 400:
            raise RuntimeError(
                "OAuth2 token endpoint returned a redirect; redirects are disabled. "
                f"status={response.status_code} host={self._safe_host()} client_id={self.client_id}"
            )

        body_bytes = self._read_capped(response)
        body_text = body_bytes.decode("utf-8", errors="replace") if body_bytes else ""

        parsed_json: Optional[dict] = None
        if body_text:
            try:
                import json as _json

                parsed_json = _json.loads(body_text)
                if not isinstance(parsed_json, dict):
                    parsed_json = None
            except ValueError:
                parsed_json = None

        if not (200 <= response.status_code < 300):
            err_code = parsed_json.get("error") if parsed_json else None
            err_desc = parsed_json.get("error_description") if parsed_json else None
            details = ""
            if err_code:
                details = f" error={err_code}"
                if err_desc:
                    details += f" error_description={err_desc}"
            raise RuntimeError(
                f"OAuth2 token endpoint returned status {response.status_code}.{details} "
                f"host={self._safe_host()} client_id={self.client_id}"
            )

        if parsed_json is None:
            raise RuntimeError(
                f"OAuth2 token endpoint returned non-JSON response. status={response.status_code} "
                f"host={self._safe_host()} client_id={self.client_id}"
            )

        access_token = parsed_json.get("access_token")
        if not access_token or not isinstance(access_token, str):
            raise RuntimeError(
                f"OAuth2 token response is missing 'access_token'. host={self._safe_host()} client_id={self.client_id}"
            )

        token_type = parsed_json.get("token_type", "Bearer")
        if not isinstance(token_type, str) or token_type.lower() != "bearer":
            raise RuntimeError(
                f"OAuth2 token response token_type '{token_type}' is not supported; only Bearer is accepted. "
                f"host={self._safe_host()} client_id={self.client_id}"
            )

        expires_in_raw = parsed_json.get("expires_in")
        try:
            expires_in = int(expires_in_raw) if expires_in_raw is not None else 0
        except (TypeError, ValueError):
            expires_in = 0

        if expires_in <= 0:
            if not self._missing_expires_in_logged:
                logger.info(
                    "OAuth2 token response omitted or returned invalid 'expires_in'; defaulting to %ss. host=%s",
                    _DEFAULT_EXPIRES_IN_SECONDS,
                    self._safe_host(),
                )
                self._missing_expires_in_logged = True
            expires_in = _DEFAULT_EXPIRES_IN_SECONDS

        expires_at = time.time() + expires_in - _SKEW_SECONDS

        return {
            "access_token": access_token,
            "token_type": token_type,
            "expires_at": expires_at,
        }

    def _read_capped(self, response: requests.Response) -> bytes:
        """Read response body up to _MAX_RESPONSE_BYTES; abort if exceeded."""
        chunks: list = []
        total = 0
        try:
            for chunk in response.iter_content(chunk_size=4096):
                if not chunk:
                    continue
                total += len(chunk)
                if total > _MAX_RESPONSE_BYTES:
                    raise RuntimeError(
                        f"OAuth2 token response exceeded {_MAX_RESPONSE_BYTES} bytes; aborting. "
                        f"host={self._safe_host()} client_id={self.client_id}"
                    )
                chunks.append(chunk)
        except requests.RequestException as exc:
            raise RuntimeError(
                f"OAuth2 token response read error. host={self._safe_host()} client_id={self.client_id}"
            ) from self._sanitize_exception(exc)
        return b"".join(chunks)

    def _read_cache(self) -> Optional[dict]:
        if self.handler_storage is not None:
            try:
                cached = self.handler_storage.encrypted_json_get(self.storage_key)
                if cached:
                    return cached
            except Exception as exc:
                logger.debug(
                    "OAuth2 token cache read failed; falling back to in-memory cache. host=%s err=%s",
                    self._safe_host(),
                    exc,
                )
        return self._memory_cache

    def _write_cache(self, token: dict) -> None:
        # Only persist the minimal token shape — never credentials or config
        cache_entry = {
            "access_token": token["access_token"],
            "token_type": token["token_type"],
            "expires_at": token["expires_at"],
        }
        self._memory_cache = cache_entry
        if self.handler_storage is not None:
            try:
                self.handler_storage.encrypted_json_set(self.storage_key, cache_entry)
            except Exception as exc:
                logger.debug(
                    "OAuth2 token cache write failed; falling back to in-memory cache. host=%s err=%s",
                    self._safe_host(),
                    exc,
                )

    @staticmethod
    def _is_expired(cached: dict) -> bool:
        expires_at = cached.get("expires_at")
        if not isinstance(expires_at, (int, float)):
            return True
        return time.time() >= expires_at

    def _safe_host(self) -> str:
        try:
            return urlparse(self.token_url).hostname or "<unknown>"
        except Exception:
            return "<unknown>"

    def _sanitize_exception(self, exc: BaseException) -> BaseException:
        """Rebuild an exception with secrets redacted from its message."""
        text = str(exc)
        redacted = text
        for secret in (self.client_secret,):
            if secret and secret in redacted:
                redacted = redacted.replace(secret, "***")
        if redacted == text:
            return exc
        return type(exc)(redacted)

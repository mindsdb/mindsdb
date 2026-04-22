"""
BearerPassthroughMixin — generic HTTP passthrough for bearer-authenticated
REST APIs.

A handler opts in by declaring three class attributes:

    class MyHandler(APIHandler, BearerPassthroughMixin):
        _bearer_token_arg = "api_key"                 # key in connection_data
        _base_url_default = "https://api.example.com" # fallback if user omits
        _test_request = PassthroughRequest("GET", "/me")

The mixin reads ``self.connection_data`` (a dict populated from
integration setup) to pull the bearer token, resolve the base URL, and
enforce the host allowlist. Handlers that need custom URL composition
(e.g. ``http://{host}:{port}``) override ``_build_base_url``.
"""

import ipaddress
import os
import time
from typing import Any
from urllib.parse import urlparse

import requests

from mindsdb.integrations.libs.passthrough_types import (
    ALLOWED_METHODS,
    FORBIDDEN_REQUEST_HEADERS,
    HOP_BY_HOP_RESPONSE_HEADERS,
    HostNotAllowedError,
    PassthroughConfigError,
    PassthroughRequest,
    PassthroughResponse,
    PassthroughValidationError,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


PASSTHROUGH_TIMEOUT_SECONDS = int(os.getenv("MINDSDB_PASSTHROUGH_TIMEOUT_SECONDS", "30"))
PASSTHROUGH_MAX_REQUEST_BYTES = int(os.getenv("MINDSDB_PASSTHROUGH_MAX_REQUEST_BYTES", str(1 * 1024 * 1024)))
PASSTHROUGH_MAX_RESPONSE_BYTES = int(os.getenv("MINDSDB_PASSTHROUGH_MAX_RESPONSE_BYTES", str(10 * 1024 * 1024)))

REDACTED_SENTINEL = "[REDACTED_API_KEY]"


def _is_private_host(hostname: str) -> bool:
    """Return True if `hostname` resolves to a private/loopback/link-local IP literal.

    Only IP literals are checked; DNS resolution is intentionally not performed
    (handlers may legitimately point at an internal DNS name the operator has
    approved via `allowed_hosts`). The IP-literal check prevents a caller from
    smuggling `http://127.0.0.1/` or `http://10.0.0.1/` through a typo'd base_url.
    """
    try:
        ip = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved


def _host_matches(host: str, allowlist: list[str]) -> bool:
    if not host:
        return False
    host = host.lower()
    return any(host == entry.lower() for entry in allowlist)


class BearerPassthroughMixin:
    # Required overrides
    _bearer_token_arg: str = ""

    # Optional overrides
    _base_url_arg: str = "base_url"
    _base_url_default: str | None = None
    _allowed_hosts_arg: str = "allowed_hosts"
    _default_headers_arg: str = "default_headers"

    # Canonical sanity-check request for `test_passthrough()`. Handlers MUST
    # set this if they want the /passthrough/test endpoint to do anything
    # useful. `None` means "test endpoint returns 'not implemented'".
    _test_request: PassthroughRequest | None = None

    # Stamped on every upstream request so the upstream can identify our
    # traffic for support/debugging. See design §13 (q3).
    _upstream_marker_header: str = "X-Minds-Passthrough"

    # Hook: override when URL composition is more than "take a string"
    # (e.g. strapi composes from host+port).
    def _build_base_url(self) -> str | None:
        data = self._get_connection_data()
        value = data.get(self._base_url_arg) if self._base_url_arg else None
        if value:
            return str(value).rstrip("/")
        if self._base_url_default is not None:
            return self._base_url_default.rstrip("/")
        return None

    def _get_connection_data(self) -> dict[str, Any]:
        """Return the handler's stored connection_data dict.

        Handlers store this differently; we check the common attribute names
        so most handlers don't need to override.
        """
        for attr in ("connection_data", "_connection_data"):
            value = getattr(self, attr, None)
            if isinstance(value, dict):
                return value
        return {}

    def _get_bearer_token(self) -> str:
        if not self._bearer_token_arg:
            raise PassthroughConfigError("handler did not declare _bearer_token_arg")
        token = self._get_connection_data().get(self._bearer_token_arg)
        if not token:
            raise PassthroughConfigError(
                f"bearer token ('{self._bearer_token_arg}') is missing from connection_data"
            )
        return str(token)

    def _resolve_url(self, path: str) -> tuple[str, str]:
        """Return ``(url, hostname)`` for the outgoing request.

        `path` is appended to the base URL verbatim. After joining we parse
        the result and compare the hostname against the allowlist — path
        injection tricks like ``@evil.com`` or ``//evil.com`` are rejected
        at the hostname-comparison step, not by string matching.
        """
        if not path.startswith("/"):
            raise PassthroughValidationError("path must start with '/'")
        base = self._build_base_url()
        if not base:
            raise PassthroughConfigError("base_url is not configured for this datasource")

        url = f"{base}{path}"
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise PassthroughValidationError(f"resolved URL is not valid: {url}")
        return url, parsed.hostname

    def _allowed_hosts(self, default_host: str) -> list[str]:
        data = self._get_connection_data()
        allowed = data.get(self._allowed_hosts_arg)
        if isinstance(allowed, list) and allowed:
            return [str(h) for h in allowed]
        return [default_host]

    def _check_host_allowed(self, hostname: str) -> None:
        allowlist = self._allowed_hosts(hostname)
        if allowlist == ["*"]:
            return
        if not _host_matches(hostname, allowlist):
            raise HostNotAllowedError(
                f"host '{hostname}' is not in the datasource allowlist"
            )
        if _is_private_host(hostname):
            raise HostNotAllowedError(
                f"host '{hostname}' resolves to a private/loopback address; "
                "add it explicitly to allowed_hosts if this is intentional"
            )

    def _build_outgoing_headers(
        self, caller_headers: dict[str, str], bearer: str
    ) -> dict[str, str]:
        """Merge caller headers (filtered) + default_headers + Authorization."""
        out: dict[str, str] = {}
        data = self._get_connection_data()
        defaults = data.get(self._default_headers_arg) or {}
        if isinstance(defaults, dict):
            out.update({str(k): str(v) for k, v in defaults.items()})
        for name, value in (caller_headers or {}).items():
            if name.lower() in FORBIDDEN_REQUEST_HEADERS:
                continue
            if name.lower().startswith("proxy-"):
                continue
            out[name] = value
        out["Authorization"] = f"Bearer {bearer}"
        out[self._upstream_marker_header] = "1"
        return out

    def _secrets_for_scrub(self) -> list[str]:
        """Values that must not appear in the response returned to the caller."""
        secrets: list[str] = []
        try:
            secrets.append(self._get_bearer_token())
        except PassthroughConfigError:
            pass
        data = self._get_connection_data()
        defaults = data.get(self._default_headers_arg) or {}
        if isinstance(defaults, dict):
            for value in defaults.values():
                s = str(value)
                if len(s) >= 16:
                    secrets.append(s)
        return secrets

    def _scrub(self, text: str, secrets: list[str]) -> str:
        for s in secrets:
            if s:
                text = text.replace(s, REDACTED_SENTINEL)
        return text

    def _filter_response_headers(self, headers: dict[str, str], secrets: list[str]) -> dict[str, str]:
        filtered: dict[str, str] = {}
        for name, value in headers.items():
            if name.lower() in HOP_BY_HOP_RESPONSE_HEADERS:
                continue
            filtered[name] = self._scrub(str(value), secrets)
        return filtered

    def _read_capped_body(self, response: requests.Response) -> bytes:
        """Read the response body in chunks, abort if it exceeds the cap."""
        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in response.iter_content(chunk_size=64 * 1024):
                if not chunk:
                    continue
                total += len(chunk)
                if total > PASSTHROUGH_MAX_RESPONSE_BYTES:
                    raise PassthroughValidationError(
                        f"response body exceeded {PASSTHROUGH_MAX_RESPONSE_BYTES} bytes"
                    )
                chunks.append(chunk)
        finally:
            response.close()
        return b"".join(chunks)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def api_passthrough(self, req: PassthroughRequest) -> PassthroughResponse:
        method = (req.method or "").upper()
        if method not in ALLOWED_METHODS:
            raise PassthroughValidationError(f"method '{req.method}' is not allowed")

        if req.body is not None:
            # requests will serialize dict bodies to JSON; we cap on the
            # serialized length. For raw strings / bytes we cap directly.
            import json as _json
            if isinstance(req.body, (dict, list)):
                body_bytes = _json.dumps(req.body).encode("utf-8")
            elif isinstance(req.body, (bytes, bytearray)):
                body_bytes = bytes(req.body)
            else:
                body_bytes = str(req.body).encode("utf-8")
            if len(body_bytes) > PASSTHROUGH_MAX_REQUEST_BYTES:
                raise PassthroughValidationError(
                    f"request body exceeded {PASSTHROUGH_MAX_REQUEST_BYTES} bytes"
                )

        url, hostname = self._resolve_url(req.path)
        self._check_host_allowed(hostname)
        bearer = self._get_bearer_token()
        headers = self._build_outgoing_headers(req.headers or {}, bearer)

        request_kwargs: dict[str, Any] = {
            "headers": headers,
            "params": req.query or None,
            "timeout": PASSTHROUGH_TIMEOUT_SECONDS,
            "stream": True,
        }
        if req.body is not None:
            if isinstance(req.body, (dict, list)):
                request_kwargs["json"] = req.body
            else:
                request_kwargs["data"] = req.body

        logger.debug(
            "passthrough %s %s (host=%s datasource=%s)",
            method, req.path, hostname, getattr(self, "name", "?"),
        )

        response = requests.request(method, url, **request_kwargs)
        body_bytes = self._read_capped_body(response)

        secrets = self._secrets_for_scrub()
        content_type = response.headers.get("Content-Type", "") or ""
        out_headers = self._filter_response_headers(dict(response.headers), secrets)

        body: Any
        if "application/json" in content_type.lower():
            try:
                text = body_bytes.decode("utf-8", errors="replace")
                text = self._scrub(text, secrets)
                import json as _json
                body = _json.loads(text) if text else None
            except ValueError:
                body = self._scrub(body_bytes.decode("utf-8", errors="replace"), secrets)
        else:
            body = self._scrub(body_bytes.decode("utf-8", errors="replace"), secrets)

        return PassthroughResponse(
            status_code=response.status_code,
            headers=out_headers,
            body=body,
            content_type=content_type.split(";", 1)[0].strip() or None,
        )

    def test_passthrough(self) -> dict[str, Any]:
        """Run the handler's canonical sanity-check call (see §6.1a).

        Returns a structured dict the HTTP layer forwards to the caller:
            { "ok": bool, "status_code": int?, "host": str?, "latency_ms": int?,
              "error_code": str?, "message": str? }
        """
        if self._test_request is None:
            return {
                "ok": False,
                "error_code": "not_implemented",
                "message": "this handler does not define a passthrough test request",
            }

        start = time.monotonic()
        try:
            resp = self.api_passthrough(self._test_request)
        except HostNotAllowedError as e:
            return {"ok": False, "error_code": e.error_code, "message": str(e)}
        except PassthroughConfigError as e:
            return {"ok": False, "error_code": e.error_code, "message": str(e)}
        except PassthroughValidationError as e:
            return {"ok": False, "error_code": e.error_code, "message": str(e)}
        except requests.exceptions.Timeout as e:
            return {"ok": False, "error_code": "timeout", "message": str(e)}
        except requests.exceptions.ConnectionError as e:
            return {"ok": False, "error_code": "network", "message": str(e)}
        except Exception as e:  # noqa: BLE001
            logger.exception("passthrough test failed unexpectedly")
            return {"ok": False, "error_code": "unknown", "message": str(e)}

        latency_ms = int((time.monotonic() - start) * 1000)
        try:
            _, host = self._resolve_url(self._test_request.path)
        except Exception:
            host = None

        if 200 <= resp.status_code < 300:
            return {"ok": True, "status_code": resp.status_code, "host": host, "latency_ms": latency_ms}
        if resp.status_code in (401, 403):
            return {
                "ok": False,
                "status_code": resp.status_code,
                "host": host,
                "latency_ms": latency_ms,
                "error_code": "auth_failed",
                "message": "upstream rejected credentials; base URL and allowlist look correct",
            }
        return {
            "ok": False,
            "status_code": resp.status_code,
            "host": host,
            "latency_ms": latency_ms,
            "error_code": "upstream_error",
            "message": f"upstream returned {resp.status_code}",
        }

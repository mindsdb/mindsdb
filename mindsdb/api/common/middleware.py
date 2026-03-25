import os
import time
import hmac
import secrets
import hashlib
from collections import deque
from http import HTTPStatus
from typing import Optional

from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.types import ASGIApp, Receive, Scope, Send

from mindsdb.utilities import log
from mindsdb.utilities.config import config

logger = log.getLogger(__name__)

SECRET_KEY = os.environ.get("AUTH_SECRET_KEY") or secrets.token_urlsafe(32)
# We store token (fingerprints) in memory, which means everyone is logged out if the process restarts
TOKENS = []


def get_pat_fingerprint(token: str) -> str:
    """Hash the token with HMAC-SHA256 using secret_key as pepper."""
    return hmac.new(SECRET_KEY.encode(), token.encode(), hashlib.sha256).hexdigest()


if config["auth"]["token"]:
    TOKENS.append(get_pat_fingerprint(config["auth"]["token"]))


def generate_pat() -> str:
    logger.debug("Generating new auth token")
    token = "pat_" + secrets.token_urlsafe(32)
    TOKENS.append(get_pat_fingerprint(token))
    return token


def verify_pat(raw_token: str) -> bool:
    """Verify if the raw_token matches a stored fingerprint.
    Returns token_id if valid, None if not.
    """
    if not raw_token:
        return False
    fp = get_pat_fingerprint(raw_token)
    for stored_fp in TOKENS:
        if hmac.compare_digest(fp, stored_fp):
            return True
    return False


def revoke_pat(raw_token: str) -> bool:
    """Revoke raw_token from active tokens"""
    if not raw_token:
        return False
    fp = get_pat_fingerprint(raw_token)
    for stored_fp in TOKENS:
        if hmac.compare_digest(fp, stored_fp):
            TOKENS.remove(stored_fp)
            return True
    return False


class PATAuthMiddleware:
    """Pure ASGI middleware (compatible with SSE / streaming responses).
    The class is not inherited from starlette.middleware.base.BaseHTTPMiddleware
    bacause it collect responses to buffer, which is not good for streaming
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    @staticmethod
    def _extract_bearer(headers: dict) -> Optional[str]:
        h = headers.get("authorization")
        if not h or not h.startswith("Bearer "):
            return None
        return h.split(" ", 1)[1].strip() or None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if config.get("auth", {}).get("http_auth_enabled", False) is False:
            await self.app(scope, receive, send)
            return

        if scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        token = self._extract_bearer(dict(request.headers))
        if not token or not verify_pat(token):
            response = JSONResponse({"detail": "Unauthorized"}, status_code=HTTPStatus.UNAUTHORIZED)
            await response(scope, receive, send)
            return

        scope.setdefault("state", {})["user"] = config["auth"].get("username")
        await self.app(scope, receive, send)


class RateLimitMiddleware:
    """Rate limiting middleware using a sliding window counter. Tracks requests per client IP."""

    def __init__(self, app: ASGIApp, requests_per_minute: int) -> None:
        self.app = app
        self.requests_per_minute = requests_per_minute
        self._window = 60.0  # seconds
        self._counters: dict[str, deque] = {}

    def _get_client_key(self, scope: Scope) -> str:
        client = scope.get("client")
        if client:
            return client[0]
        return "unknown"

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if scope.get("method") == "OPTIONS":
            await self.app(scope, receive, send)
            return

        # Clients usually repeat this request until
        # the connection is established, so no rate limit it.
        if scope.get("method") == "GET" and scope.get("path", "").endswith("/sse"):
            await self.app(scope, receive, send)
            return

        client_key = self._get_client_key(scope)
        now = time.monotonic()
        window_start = now - self._window

        timestamps = self._counters.setdefault(client_key, deque())

        # Evict timestamps outside the current window
        while timestamps and timestamps[0] <= window_start:
            timestamps.popleft()

        if len(timestamps) >= self.requests_per_minute:
            retry_after = int(self._window - (now - timestamps[0])) + 1
        else:
            retry_after = None
            timestamps.append(now)

        if retry_after is not None:
            response = JSONResponse(
                {"detail": f"Too Many Requests, retry after {retry_after} seconds"},
                status_code=HTTPStatus.TOO_MANY_REQUESTS,
                headers={"Retry-After": str(retry_after)},
            )
            await response(scope, receive, send)
            return

        stale_keys = [k for k, ts in self._counters.items() if not ts or ts[-1] <= window_start]
        for k in stale_keys:
            del self._counters[k]

        await self.app(scope, receive, send)


# Used by mysql protocol
def check_auth(username, password, scramble_func, salt, company_id, user_id, config):
    try:
        hardcoded_user = config["auth"].get("username")
        hardcoded_password = config["auth"].get("password")
        if hardcoded_password is None:
            hardcoded_password = ""
        hardcoded_password_hash = scramble_func(hardcoded_password, salt)
        hardcoded_password = hardcoded_password.encode()

        if password is None:
            password = ""
        if isinstance(password, str):
            password = password.encode()

        if username != hardcoded_user:
            logger.warning(f"Check auth, user={username}: user mismatch")
            return {"success": False}

        if password != hardcoded_password and password != hardcoded_password_hash:
            logger.warning(f"check auth, user={username}: password mismatch")
            return {"success": False}

        logger.info(f"Check auth, user={username}: Ok")
        return {"success": True, "username": username, "company_id": company_id, "user_id": user_id}
    except Exception:
        logger.exception(f"Check auth, user={username}: ERROR")

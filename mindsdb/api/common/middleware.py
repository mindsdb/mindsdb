import os
import hmac
import secrets
import hashlib
from http import HTTPStatus
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request

from mindsdb.utilities import log
from mindsdb.utilities.config import config

logger = log.getLogger(__name__)

SECRET_KEY = os.environ.get("AUTH_SECRET_KEY") or secrets.token_urlsafe(32)
# We store token (fingerprints) in memory, which means everyone is logged out if the process restarts
TOKENS = []


def get_pat_fingerprint(token: str) -> str:
    """Hash the token with HMAC-SHA256 using secret_key as pepper."""
    return hmac.new(SECRET_KEY.encode(), token.encode(), hashlib.sha256).hexdigest()


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


class PATAuthMiddleware(BaseHTTPMiddleware):
    def _extract_bearer(self, request: Request) -> Optional[str]:
        h = request.headers.get("Authorization")
        if not h or not h.startswith("Bearer "):
            return None
        return h.split(" ", 1)[1].strip() or None

    async def dispatch(self, request: Request, call_next):
        if config.get("auth", {}).get("http_auth_enabled", False) is False:
            return await call_next(request)

        token = self._extract_bearer(request)
        if not token or not verify_pat(token):
            return JSONResponse({"detail": "Unauthorized"}, status_code=HTTPStatus.UNAUTHORIZED)

        request.state.user = config["auth"].get("username")
        return await call_next(request)


# Used by mysql protocol
def check_auth(username, password, scramble_func, salt, company_id, config):
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
        return {"success": True, "username": username}
    except Exception:
        logger.exception(f"Check auth, user={username}: ERROR")

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse
from starlette.requests import Request
from http import HTTPStatus
import secrets
import os

from mindsdb.utilities import log
from mindsdb.utilities.config import config
from typing import Optional

logger = log.getLogger(__name__)

SECRET_KEY = os.environ.get("AUTH_SECRET_KEY") or secrets.token_hex(32)
SIMPLE_USER_TOKEN = None


def new_token() -> str:
    global SIMPLE_USER_TOKEN
    logger.debug("Generating new auth token")
    SIMPLE_USER_TOKEN = secrets.token_urlsafe(32)
    return SIMPLE_USER_TOKEN


def verify_token(token: str) -> None:
    return SIMPLE_USER_TOKEN is not None and token == SIMPLE_USER_TOKEN


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
        if not token or not verify_token(token):
            return JSONResponse({"detail": "Unauthorized"}, status_code=HTTPStatus.UNAUTHORIZED)

        request.state.user = config["auth"].get("username")
        return await call_next(request)

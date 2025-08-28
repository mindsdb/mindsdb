from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.status import HTTP_401_UNAUTHORIZED

from mindsdb.utilities.a2a_auth import validate_auth_token_remote, get_auth_required_status
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class A2AAuthMiddleware(BaseHTTPMiddleware):
    """Middleware to handle A2A authentication based on HTTP auth configuration"""

    async def dispatch(self, request: Request, call_next):
        # Check if authentication is required
        if not get_auth_required_status():
            # No authentication required, proceed
            response = await call_next(request)
            return response

        # Check for Authorization header
        auth_header = request.headers.get("Authorization")
        if not auth_header:
            logger.warning("A2A request without Authorization header")
            return JSONResponse(
                status_code=HTTP_401_UNAUTHORIZED,
                content={
                    "error": "Unauthorized",
                    "message": "Authorization header is required"
                }
            )

        # Extract token from Authorization header
        if not auth_header.startswith("Bearer "):
            logger.warning("A2A request with invalid Authorization header format")
            return JSONResponse(
                status_code=HTTP_401_UNAUTHORIZED,
                content={
                    "error": "Unauthorized",
                    "message": "Authorization header must start with 'Bearer '"
                }
            )

        token = auth_header[7:]  # Remove "Bearer " prefix
        
        # Validate token remotely via HTTP API
        if not validate_auth_token_remote(token):
            logger.warning(f"A2A request with invalid token: {token[:8]}...{token[-8:] if len(token) > 16 else '***'}")
            return JSONResponse(
                status_code=HTTP_401_UNAUTHORIZED,
                content={
                    "error": "Unauthorized",
                    "message": "Invalid or expired token"
                }
            )

        # Token is valid, proceed with request
        logger.debug(f"A2A request authenticated with token: {token[:8]}...{token[-8:]}")
        response = await call_next(request)
        return response

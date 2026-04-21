from typing import Any
from urllib.parse import urljoin

import httpx
from pydantic import AnyHttpUrl
from mcp.server.auth.settings import AuthSettings
from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.shared.auth_utils import check_resource_allowed, resource_url_from_server_url

from mindsdb.utilities.config import config
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class IntrospectionTokenVerifier(TokenVerifier):
    """Token verifier that uses OAuth 2.0 Token Introspection (RFC 7662).
    Intended for use when MindsDB acts as a Resource Server and token
    issuance is delegated to an external provider (e.g. Keycloak).

    Args:
        introspection_endpoint: Full URL of the RFC 7662 introspection endpoint.
        server_url: Public URL of this MCP server (e.g. ``http://host:port/mcp/streamable``).
            Used to derive the expected ``aud`` (audience) claim value.
        client_id: OAuth client ID used to authenticate against the introspection endpoint.
        client_secret: OAuth client secret used to authenticate against the introspection endpoint.
    """

    def __init__(
        self,
        introspection_endpoint: str,
        server_url: str,
        client_id: str,
        client_secret: str,
    ):
        self.introspection_endpoint = introspection_endpoint
        self.server_url = server_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.resource_url = resource_url_from_server_url(server_url)

    async def verify_token(self, token: str) -> AccessToken | None:
        """Verify a bearer token via the introspection endpoint.

        Args:
            token: Raw bearer token string extracted from the Authorization header.

        Returns:
            AccessToken: Populated access token on successful verification.
            None: If the token is inactive, the audience is invalid, the endpoint
                  is unreachable, or any other error occurs.
        """
        # to prevent SSRF attacks it must start from https, or be local server
        if not self.introspection_endpoint.startswith(("https://", "http://localhost:", "http://127.0.0.1:")):
            return None

        timeout = httpx.Timeout(10.0, connect=5.0)
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            verify=True,
            follow_redirects=False,
        ) as client:
            try:
                form_data = {
                    "token": token,
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                }
                headers = {"Content-Type": "application/x-www-form-urlencoded"}

                response = await client.post(
                    self.introspection_endpoint,
                    data=form_data,
                    headers=headers,
                )

                if response.status_code != 200:
                    return None

                data = response.json()
                if not data.get("active", False):
                    return None

                if not self._validate_resource(data):
                    return None

                return AccessToken(
                    token=token,
                    client_id=data.get("client_id", "unknown"),
                    scopes=data.get("scope", "").split() if data.get("scope") else [],
                    expires_at=data.get("exp"),
                    resource=self.resource_url,
                )

            except Exception as e:
                logger.error(f"Error during token verification: {e}")
                return None

    def _validate_resource(self, token_data: dict[str, Any]) -> bool:
        """Validate that the token was issued for this resource server (RFC 8707).

        Args:
            token_data: Parsed JSON response from the introspection endpoint.

        Returns:
            bool: True if at least one audience entry matches this server's resource URL,
                  False if ``aud`` is missing or no entry matches.
        """
        if not self.server_url or not self.resource_url:
            return False

        aud: list[str] | str | None = token_data.get("aud")
        if isinstance(aud, list):
            return any(check_resource_allowed(self.resource_url, a) for a in aud)
        if isinstance(aud, str):
            return check_resource_allowed(self.resource_url, aud)
        return False


def build_oauth_components() -> tuple[IntrospectionTokenVerifier, AuthSettings] | tuple[None, None]:
    """Build token verifier and auth settings from the OAuth config section.

    Returns:
        tuple[IntrospectionTokenVerifier, AuthSettings]: Token verifier and auth settings ready
                                                         to pass to FastMCP if OAuth is enabled.
        tuple[None, None]: If OAuth ``enabled`` is False or not set.
    """
    oauth_cfg = config["api"]["mcp"]["oauth"]
    if not oauth_cfg.get("enabled", False):
        return None, None

    public_url = oauth_cfg.get("public_url", "").rstrip("/")
    if public_url:
        mcp_endpoint_url = f"{public_url}/mcp/streamable"
    else:
        host = config["api"]["http"]["host"]
        port = config["api"]["http"]["port"]
        # Bind-all addresses (0.0.0.0 / ::) are not valid client-facing destinations.
        # Replace with loopback so the advertised resource_metadata URL is reachable.
        if host in ("0.0.0.0", "", "::"):
            host = "127.0.0.1"
        mcp_endpoint_url = f"http://{host}:{port}/mcp/streamable"

    issuer_url = oauth_cfg.get("issuer_url", "").rstrip("/") + "/"
    client_id = oauth_cfg.get("client_id", "")
    client_secret = oauth_cfg.get("client_secret", "")
    scope = oauth_cfg.get("scope", "mcp:tools")

    introspection_endpoint = urljoin(issuer_url, "protocol/openid-connect/token/introspect")

    token_verifier = IntrospectionTokenVerifier(
        introspection_endpoint=introspection_endpoint,
        server_url=mcp_endpoint_url,
        client_id=client_id,
        client_secret=client_secret,
    )

    auth_settings = AuthSettings(
        issuer_url=AnyHttpUrl(issuer_url),
        required_scopes=[scope],
        resource_server_url=AnyHttpUrl(mcp_endpoint_url),
    )

    return token_verifier, auth_settings

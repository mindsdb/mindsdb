from typing import Any
from mcp.server.fastmcp import FastMCP

from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.shared.auth_utils import check_resource_allowed, resource_url_from_server_url

class Cfg:
    """Configuration class that loads from environment variables with sensible defaults."""

    # Server settings
    HOST: str = "127.0.0.1" #os.getenv("HOST", "localhost")
    PORT: int = 47334 #int(os.getenv("PORT", "3000"))

    # Auth server settings
    AUTH_HOST: str = "localhost"
    AUTH_PORT: int = 8080
    AUTH_REALM: str = "master"

    # OAuth client settings
    OAUTH_CLIENT_ID: str = "mcptest"
    OAUTH_CLIENT_SECRET: str = "test"

    # Server settings
    MCP_SCOPE: str = "mcp:tools"
    OAUTH_STRICT: bool = False # os.getenv("OAUTH_STRICT", "false").lower() in ("true", "1", "yes")
    TRANSPORT: str = "streamable-http"

    # Mount path used in start.py: Mount("/mcp", ...) and streamable_http_path="/streamable"
    MCP_MOUNT_PATH: str = "/mcp"
    MCP_STREAMABLE_PATH: str = "/streamable"

    @property
    def server_url(self) -> str:
        """Build the server URL."""
        return f"http://{self.HOST}:{self.PORT}"

    @property
    def mcp_endpoint_url(self) -> str:
        """Full URL of the MCP streamable HTTP endpoint.

        RFC 9728 requires the 'resource' field in Protected Resource Metadata
        to match what the MCP client uses as the server URL, so we use the
        specific endpoint path rather than just the server root.
        """
        return f"{self.server_url}{self.MCP_MOUNT_PATH}{self.MCP_STREAMABLE_PATH}"

    @property
    def auth_base_url(self) -> str:
        """Build the auth server base URL."""
        return f"http://{self.AUTH_HOST}:{self.AUTH_PORT}/realms/{self.AUTH_REALM}/"

    def validate(self) -> None:
        """Validate configuration."""
        if self.TRANSPORT not in ["sse", "streamable-http"]:
            raise ValueError(f"Invalid transport: {self.TRANSPORT}. Must be 'sse' or 'streamable-http'")


config = Cfg()

class IntrospectionTokenVerifier(TokenVerifier):
    """Token verifier that uses OAuth 2.0 Token Introspection (RFC 7662).
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
        """Verify token via introspection endpoint."""
        import httpx

        if not self.introspection_endpoint.startswith(("https://", "http://localhost", "http://127.0.0.1")):
            return None

        timeout = httpx.Timeout(10.0, connect=5.0)
        limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)

        async with httpx.AsyncClient(
            timeout=timeout,
            limits=limits,
            verify=True,
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
                    resource=data.get("aud"),  # Include resource in token
                )

            except Exception as e:
                return None

    def _validate_resource(self, token_data: dict[str, Any]) -> bool:
        """Validate token was issued for this resource server.

        Rules:
        - Reject if 'aud' missing.
        - Accept if any audience entry matches the derived resource URL.
        - Supports string or list forms per JWT spec.
        """
        if not self.server_url or not self.resource_url:
            return False

        aud: list[str] | str | None = token_data.get("aud")
        if isinstance(aud, list):
            return any(self._is_valid_resource(a) for a in aud)
        if isinstance(aud, str):
            return self._is_valid_resource(aud)
        return False

    def _is_valid_resource(self, resource: str) -> bool:
        """Check if the given resource matches our server."""
        return check_resource_allowed(self.resource_url, resource)


def create_oauth_urls() -> dict[str, str]:
    """Create OAuth URLs based on configuration (Keycloak-style)."""
    from urllib.parse import urljoin

    auth_base_url = config.auth_base_url

    return {
        "issuer": auth_base_url,
        "introspection_endpoint": urljoin(auth_base_url, "protocol/openid-connect/token/introspect"),
        "authorization_endpoint": urljoin(auth_base_url, "protocol/openid-connect/auth"),
        "token_endpoint": urljoin(auth_base_url, "protocol/openid-connect/token"),
    }


oauth_urls = create_oauth_urls()


token_verifier = IntrospectionTokenVerifier(
    introspection_endpoint=oauth_urls["introspection_endpoint"],
    server_url=config.mcp_endpoint_url,
    client_id=config.OAUTH_CLIENT_ID,
    client_secret=config.OAUTH_CLIENT_SECRET,
)


from pydantic import AnyHttpUrl
from mcp.server.auth.settings import AuthSettings

mcp = FastMCP(
    name="MindsDB",
    dependencies=["mindsdb"],
    streamable_http_path=config.MCP_STREAMABLE_PATH,
    debug=True,
    token_verifier=token_verifier,
    auth=AuthSettings(
        issuer_url=AnyHttpUrl(oauth_urls["issuer"]),
        required_scopes=[config.MCP_SCOPE],
        resource_server_url=AnyHttpUrl(config.mcp_endpoint_url),
    ),
)

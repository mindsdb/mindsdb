import time
import urllib.parse
from typing import Optional

from flask import request
from hubspot import HubSpot
from hubspot.utils.oauth import get_auth_url

from mindsdb.utilities import log
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

logger = log.getLogger(__name__)

_STORAGE_KEY = "hubspot_oauth_tokens"
_DEFAULT_REDIRECT_PATH = "/verify-auth"
_TOKEN_EXPIRY_BUFFER = 0.95


class HubSpotOAuth2Manager:
    """
    Manages HubSpot OAuth2 authorization_code flow for MindsDB.
    """

    def __init__(
        self,
        handler_storage,
        client_id: str,
        client_secret: str,
        scopes: Optional[str] = None,
        optional_scopes: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        code: Optional[str] = None,
        datasource_name: Optional[str] = None,
    ) -> None:
        self.handler_storage = handler_storage
        self.client_id = client_id
        self.client_secret = client_secret
        self.scopes = tuple(scopes.split()) if scopes else ("oauth",)
        self.optional_scopes = tuple(optional_scopes.split()) if optional_scopes else None
        self.redirect_uri = redirect_uri
        self.code = code
        self.datasource_name = datasource_name

    def get_access_token(self) -> str:
        """
        Return a valid HubSpot access token.
        Raises:
            AuthException: User authorization required; auth_url is attached.
        """
        stored = self.handler_storage.encrypted_json_get(_STORAGE_KEY)
        logger.debug(f"Retrieved stored token data: {stored}")
        if stored:
            if time.time() < stored.get("expires_at", 0):
                return stored["access_token"]

            if stored.get("refresh_token"):
                try:
                    return self._refresh_token(stored["refresh_token"])
                except Exception as e:
                    logger.warning("HubSpot token refresh failed, reauthorization required: %s", e)

        runtime_code = self._get_runtime_code()
        if runtime_code:
            try:
                return self._exchange_code(runtime_code)
            except Exception as e:
                # OAuth codes are single-use and expire quickly.
                # If the exchange fails (BAD_AUTH_CODE), don't retry — prompt re-authorization.
                logger.warning("HubSpot code exchange failed (code may be expired/used): %s", e)

        redirect_uri = self._get_redirect_uri()
        auth_url = get_auth_url(
            scope=self.scopes,
            optional_scope=self.optional_scopes,
            client_id=self.client_id,
            redirect_uri=redirect_uri,
        )
        # Fix for HubSpot's strict URL parsing. Python's URL encode translates spaces to `+`, but
        # HubSpot's optional_scopes requires `%20` or `,`.
        auth_url = auth_url.replace("+", "%20")

        # Append state with datasource info so the frontend can complete the connection
        # even when localStorage context is missing (e.g. script-initiated flows).
        if self.datasource_name:
            state_data = urllib.parse.urlencode(
                {
                    "datasource_name": self.datasource_name,
                    "integrations_name": "hubspot",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                    "redirect_uri": redirect_uri,
                    "scope": " ".join(self.scopes) if self.scopes else "oauth",
                    "optional_scope": " ".join(self.optional_scopes) if self.optional_scopes else "",
                }
            )
            auth_url += f"&state={urllib.parse.quote(state_data)}"

        raise AuthException(
            f"HubSpot authorization required. Please visit: {auth_url}",
            auth_url=auth_url,
        )

    def _get_runtime_code(self) -> Optional[str]:
        """Return the OAuth authorization code from explicit value or active request context."""
        if self.code:
            return self.code
        try:
            return request.args.get("code")
        except RuntimeError:
            return None

    def _exchange_code(self, code: str) -> str:
        """Exchange an authorization code for access and refresh tokens."""
        response = HubSpot().oauth.tokens_api.create(
            grant_type="authorization_code",
            code=code,
            redirect_uri=self._get_redirect_uri(),
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return self._persist_tokens(response)

    def _refresh_token(self, refresh_token: str) -> str:
        """Obtain a new access token using the stored refresh token."""
        response = HubSpot().oauth.tokens_api.create(
            grant_type="refresh_token",
            refresh_token=refresh_token,
            redirect_uri=self._get_redirect_uri(),
            client_id=self.client_id,
            client_secret=self.client_secret,
        )
        return self._persist_tokens(response)

    def _persist_tokens(self, token_response) -> str:
        """Save token data to encrypted handler storage and return the access token."""
        tokens = {
            "access_token": token_response.access_token,
            "refresh_token": token_response.refresh_token,
            "expires_at": time.time() + token_response.expires_in * _TOKEN_EXPIRY_BUFFER,
        }
        self.handler_storage.encrypted_json_set(_STORAGE_KEY, tokens)
        return tokens["access_token"]

    def _get_redirect_uri(self) -> str:
        if self.redirect_uri:
            return self.redirect_uri
        try:
            origin = request.headers.get("ORIGIN", "http://localhost:47334")
        except RuntimeError:
            origin = "http://localhost:47334"
        return origin + _DEFAULT_REDIRECT_PATH

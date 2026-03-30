"""LinkedIn Ads OAuth token management.

Handles token lifecycle including refresh, storage, validation, and expiry checking.
"""

from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from mindsdb.utilities import log

logger = log.getLogger(__name__)


class LinkedInAdsAuthManager:
    """Manages OAuth token lifecycle for LinkedIn Ads API.

    Responsibilities:
    - Token storage and retrieval via handler_storage
    - Token refresh using OAuth refresh token flow
    - Token expiry validation with configurable skew
    - Thread-safe token refresh operations
    """

    TOKEN_ENDPOINT = "https://www.linkedin.com/oauth/v2/accessToken"
    TOKEN_STORAGE_KEY = "linkedin_ads_tokens"
    TOKEN_EXPIRY_SKEW_SECONDS = 300  # 5 minutes buffer before actual expiry

    def __init__(
        self,
        handler_storage: Any,
        client_id: str | None,
        client_secret: str | None,
        access_token: str | None = None,
        refresh_token: str | None = None,
    ):
        """Initialize auth manager.

        Args:
            handler_storage: MindsDB handler storage for encrypted token persistence
            client_id: LinkedIn OAuth client ID for token refresh
            client_secret: LinkedIn OAuth client secret for token refresh
            access_token: Initial access token from connection_data (optional)
            refresh_token: Refresh token for obtaining new access tokens (optional)
        """
        self.handler_storage = handler_storage
        self.client_id = client_id
        self.client_secret = client_secret
        self.initial_access_token = access_token
        self.initial_refresh_token = refresh_token
        self._refresh_lock = threading.Lock()

    def get_valid_access_token(self) -> str:
        """Get a valid access token, refreshing if necessary.

        Returns:
            Valid access token string

        Raises:
            ValueError: If no valid token can be obtained
        """
        token_data = self._get_valid_token_data()
        access_token = token_data.get("access_token")
        if not access_token:
            raise ValueError("A valid access_token could not be obtained")
        return access_token

    def _get_valid_token_data(self) -> dict[str, Any]:
        """Get valid token data, refreshing if expired.

        Returns:
            Dictionary containing access_token, refresh_token, and expiry metadata
        """
        # Try to load from storage first
        stored_token_data = self._load_stored_tokens()
        if stored_token_data:
            token_data = stored_token_data
        else:
            # First time - use connection_data tokens
            if not self.initial_access_token and not self.initial_refresh_token:
                raise ValueError("At least access_token or refresh_token must be provided for authentication")
            token_data = {
                "access_token": self.initial_access_token,
                "refresh_token": self.initial_refresh_token,
                "expires_at": None,
                "refresh_token_expires_at": None,
            }
            self._store_tokens(token_data)

        # Refresh if expired
        if self._token_is_expired(token_data) and token_data.get("refresh_token"):
            if not self.client_id or not self.client_secret:
                raise ValueError("client_id and client_secret are required to refresh an expired LinkedIn token")
            with self._refresh_lock:
                # Double-check after acquiring lock
                latest_tokens = self._load_stored_tokens() or token_data
                if self._token_is_expired(latest_tokens):
                    token_data = self._refresh_tokens(latest_tokens["refresh_token"])
                    self._store_tokens(token_data)
                else:
                    token_data = latest_tokens
        # If no access token but have refresh token, get access token
        elif not token_data.get("access_token") and token_data.get("refresh_token"):
            if not self.client_id or not self.client_secret:
                raise ValueError("client_id and client_secret are required to exchange a LinkedIn refresh token")
            with self._refresh_lock:
                token_data = self._refresh_tokens(token_data["refresh_token"])
                self._store_tokens(token_data)

        return token_data

    def refresh_if_needed(self) -> dict[str, Any]:
        """Force token refresh and return new token data.

        Returns:
            New token data after refresh

        Raises:
            ValueError: If refresh credentials are missing
        """
        stored_tokens = self._load_stored_tokens()
        if not stored_tokens:
            raise ValueError("No stored tokens found for refresh")

        refresh_token = stored_tokens.get("refresh_token")
        if not refresh_token:
            raise ValueError("No refresh_token available for LinkedIn Ads token refresh")
        if not self.client_id or not self.client_secret:
            raise ValueError("client_id and client_secret are required for LinkedIn Ads token refresh")

        with self._refresh_lock:
            token_data = self._refresh_tokens(refresh_token)
            self._store_tokens(token_data)
            return token_data

    def _refresh_tokens(self, refresh_token: str) -> dict[str, Any]:
        """Exchange refresh token for new access token.

        Args:
            refresh_token: The refresh token to exchange

        Returns:
            Dictionary with new access_token, refresh_token, and expiry metadata

        Raises:
            RuntimeError: If LinkedIn API returns an error
        """
        response = requests.post(
            self.TOKEN_ENDPOINT,
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        if not response.ok:
            raise RuntimeError(f"LinkedIn Ads token refresh failed {response.status_code}: {response.text}")

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Unexpected LinkedIn token refresh response format")

        # LinkedIn may or may not return a new refresh token
        new_refresh_token = payload.get("refresh_token") or refresh_token
        return {
            "access_token": payload.get("access_token"),
            "refresh_token": new_refresh_token,
            "expires_at": self._build_expiry(payload.get("expires_in")),
            "refresh_token_expires_at": self._build_expiry(payload.get("refresh_token_expires_in")),
        }

    def _load_stored_tokens(self) -> dict[str, Any] | None:
        """Load tokens from handler storage.

        Returns:
            Token data dictionary or None if not found
        """
        if self.handler_storage is None:
            return None
        try:
            payload = self.handler_storage.encrypted_json_get(self.TOKEN_STORAGE_KEY)
        except Exception as exc:  # noqa: BLE001
            logger.debug("No stored LinkedIn Ads tokens found: %s", exc)
            return None
        if not isinstance(payload, dict):
            return None
        return payload

    def _store_tokens(self, token_data: dict[str, Any]) -> None:
        """Store tokens in handler storage.

        Args:
            token_data: Token data to store (datetime objects will be converted to ISO format)
        """
        if self.handler_storage is None:
            return
        payload = dict(token_data)
        # Convert datetime objects to ISO strings for JSON storage
        for key in ("expires_at", "refresh_token_expires_at"):
            value = payload.get(key)
            if isinstance(value, datetime):
                payload[key] = value.isoformat()
        self.handler_storage.encrypted_json_set(self.TOKEN_STORAGE_KEY, payload)

    def _token_is_expired(self, token_data: dict[str, Any]) -> bool:
        """Check if token is expired or will expire soon.

        Args:
            token_data: Token data containing expires_at field

        Returns:
            True if token is expired or will expire within TOKEN_EXPIRY_SKEW_SECONDS
        """
        expires_at = token_data.get("expires_at")
        if not expires_at:
            return False

        expiry = expires_at
        if isinstance(expires_at, str):
            expiry = datetime.fromisoformat(expires_at)
        if isinstance(expiry, datetime):
            if expiry.tzinfo is None:
                expiry = expiry.replace(tzinfo=timezone.utc)
            # Add skew buffer to avoid using tokens that are about to expire
            now = datetime.now(timezone.utc) + timedelta(seconds=self.TOKEN_EXPIRY_SKEW_SECONDS)
            return expiry <= now
        return False

    @staticmethod
    def _build_expiry(seconds: Any) -> datetime | None:
        """Build expiry datetime from seconds.

        Args:
            seconds: Number of seconds until expiry

        Returns:
            Expiry datetime or None if seconds is invalid
        """
        if not seconds:
            return None
        try:
            return datetime.now(timezone.utc) + timedelta(seconds=int(seconds))
        except (TypeError, ValueError):
            return None

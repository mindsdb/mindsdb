import requests
import base64
import threading
from typing import Optional, Dict, Any
from datetime import datetime, timedelta, timezone
import json

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

from xero_python.identity import IdentityApi
from xero_python.api_client import ApiClient, Configuration
from xero_python.api_client.configuration import OAuth2Token

from .tables.accounts_table import AccountsTable
from .tables.bank_transactions_table import BankTransactionsTable
from .tables.budgets_table import BudgetsTable
from .tables.contact_groups_table import ContactGroupsTable
from .tables.contacts_table import ContactsTable
from .tables.quotes_table import QuotesTable
from .tables.credit_notes_table import CreditNotesTable
from .tables.invoices_table import InvoicesTable
from .tables.items_table import ItemsTable
from .tables.journals_table import JournalsTable
from .tables.manual_journals_table import ManualJournalsTable
from .tables.organisations_table import OrganisationsTable
from .tables.overpayments_table import OverpaymentsTable
from .tables.payments_table import PaymentsTable

class XeroHandler(APIHandler):
    """
    Xero Handler for MindsDB

    Implements OAuth2 authentication with Xero API and provides read-only access
    to accounting data including budgets, contacts, invoices, items, and more.
    """

    name = 'xero'

    # Class-level lock to prevent concurrent token refresh attempts (race condition protection)
    _refresh_lock = threading.Lock()

    def __init__(self, name: str, **kwargs):
        """
        Initialize the Xero handler

        Args:
            name: Handler name
            **kwargs: Additional arguments including connection_data and handler_storage
        """
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data", {})
        self.handler_storage = kwargs.get("handler_storage")
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.tenant_id = None
        self.api_client = None

        # OAuth2 configuration
        self.client_id = self.connection_data.get("client_id")
        self.client_secret = self.connection_data.get("client_secret")
        self.redirect_uri = self.connection_data.get("redirect_uri")
        self.code = self.connection_data.get("code")

        # Register tables
        self._register_table("accounts", AccountsTable(self))
        self._register_table("bank_transactions", BankTransactionsTable(self))
        self._register_table("budgets", BudgetsTable(self))
        self._register_table("contact_groups", ContactGroupsTable(self))
        self._register_table("contacts", ContactsTable(self))
        self._register_table("credit_notes", CreditNotesTable(self))
        self._register_table("invoices", InvoicesTable(self))
        self._register_table("items", ItemsTable(self))
        self._register_table("journals", JournalsTable(self))
        self._register_table("manual_journals", ManualJournalsTable(self))
        self._register_table("organisations", OrganisationsTable(self))
        self._register_table("overpayments", OverpaymentsTable(self))
        self._register_table("payments", PaymentsTable(self))
        # self._register_table("purchase_orders", PurchaseOrdersTable(self))
        self._register_table("quotes", QuotesTable(self))
        # self._register_table("repeating_invoices", RepeatingInvoicesTable(self))

    def _use_token_injection_path(self) -> bool:
        """
        Determine if using token injection (backend) or code flow (direct).

        Returns:
            bool: True if access_token or refresh_token provided, False for code flow
        """
        return "access_token" in self.connection_data or "refresh_token" in self.connection_data

    def connect(self) -> ApiClient:
        """
        Establish connection to Xero API with OAuth2 authentication.

        Supports two authentication paths:
        1. Token Injection (backend): Uses provided access_token/refresh_token
        2. Code Flow (direct): OAuth2 authorization code exchange

        Returns:
            ApiClient: The configured Xero API client
        """
        if self.is_connected and self.connection is not None:
            return self.connection

        try:
            # Choose authentication path
            if self._use_token_injection_path():
                # Modern path: Token injection from backend
                token_data = self._connect_with_token_injection()
            else:
                # Legacy path: Code exchange flow
                token_data = self._connect_with_code_exchange()

            # Set tenant_id if provided or use stored one
            self.tenant_id = self.connection_data.get("tenant_id") or token_data.get("tenant_id")

            # Create API client with access token
            self._setup_api_client(token_data["access_token"])
            self.is_connected = True

        except AuthException:
            raise
        except Exception as e:
            raise Exception(f"Failed to connect to Xero: {str(e)}")

        return self.connection

    def _connect_with_token_injection(self) -> Dict[str, Any]:
        """
        Handle authentication via token injection from backend systems.

        Supports:
        - Loading stored tokens from previous refresh operations (most important for rotating refresh tokens!)
        - Falling back to connection_data tokens if no stored tokens available
        - Automatic refresh if refresh_token and client credentials provided
        - Race condition protection: only one thread refreshes tokens at a time
        - Grace period of 5 minutes before token expiry
        - Token refresh skipped if client credentials not available (use access_token as-is)

        **Rotating Refresh Token Pattern:**
        Xero invalidates refresh tokens after each use and returns a new one. This method:
        1. Tries to load previously stored tokens (which contain the latest refresh token)
        2. Falls back to connection_data tokens only for initial setup
        3. Uses a lock to prevent concurrent refresh attempts (critical for rotating tokens)

        Returns:
            dict: Token data with access_token, refresh_token, expires_at, tenant_id
        """
        # Step 1: Try to load previously stored tokens first
        # This is CRITICAL for rotating refresh tokens - stored tokens have the latest refresh token
        stored_token_data = self._load_stored_tokens()

        if stored_token_data:
            token_data = stored_token_data
        else:
            # No stored tokens - use provided tokens from connection data
            access_token = self.connection_data.get("access_token")
            refresh_token = self.connection_data.get("refresh_token")
            expires_at = self.connection_data.get("expires_at")

            if not access_token and not refresh_token:
                raise ValueError("At least access_token or refresh_token must be provided for token injection")

            # Build initial token data
            token_data = {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "expires_at": expires_at,
                "tenant_id": self.connection_data.get("tenant_id"),
            }

            # Store initial tokens so next connection uses them (important for rotating refresh tokens)
            self._store_tokens(token_data)

        # Step 2: Check if token needs refresh with race condition protection
        if self._is_token_expired(token_data) and token_data.get("refresh_token"):
            if self.client_id and self.client_secret:
                # Acquire lock to prevent concurrent token refresh attempts
                with self._refresh_lock:
                    # Double-check pattern: re-check stored tokens after acquiring lock
                    # Another thread may have already refreshed the token
                    stored_token_data = self._load_stored_tokens()
                    if stored_token_data and not self._is_token_expired(stored_token_data):
                        # Token was refreshed by another thread while we waited for the lock
                        token_data = stored_token_data
                    else:
                        # Proceed with refresh
                        token_data = self._refresh_tokens(token_data["refresh_token"])
                        self._store_tokens(token_data)

                        # Update connection_data for current session to use new tokens
                        self.connection_data["access_token"] = token_data["access_token"]
                        if "refresh_token" in token_data:
                            self.connection_data["refresh_token"] = token_data["refresh_token"]
                        if "expires_at" in token_data:
                            self.connection_data["expires_at"] = token_data["expires_at"]
            else:
                # No credentials available for refresh - warn but continue with expired token
                # The API call will fail if token is truly invalid
                pass
        elif not token_data.get("access_token") and token_data.get("refresh_token"):
            # No access token but have refresh token - try to get new one
            if self.client_id and self.client_secret:
                with self._refresh_lock:
                    # Double-check after acquiring lock
                    stored_token_data = self._load_stored_tokens()
                    if stored_token_data and stored_token_data.get("access_token"):
                        token_data = stored_token_data
                    else:
                        token_data = self._refresh_tokens(token_data["refresh_token"])
                        self._store_tokens(token_data)

                        # Update connection_data for current session
                        self.connection_data["access_token"] = token_data["access_token"]
                        if "refresh_token" in token_data:
                            self.connection_data["refresh_token"] = token_data["refresh_token"]
            else:
                raise ValueError(
                    "Cannot refresh token: access_token is missing and client credentials (client_id/client_secret) "
                    "are not provided. Please provide either a valid access_token or both client credentials."
                )

        return token_data

    def _connect_with_code_exchange(self) -> Dict[str, Any]:
        """
        Handle traditional OAuth2 code flow authentication.

        Flow:
        1. Check for stored tokens
        2. Refresh if expired
        3. Exchange code if provided
        4. Raise AuthException if no tokens/code available

        Returns:
            dict: Token data with access_token, refresh_token, expires_at, tenant_id
        """
        # Try to load existing tokens from storage
        token_data = self._load_stored_tokens()

        if token_data:
            # Check if token is expired
            if self._is_token_expired(token_data):
                token_data = self._refresh_tokens(token_data["refresh_token"])
                self._store_tokens(token_data)
            return token_data

        if self.code:
            # Exchange authorization code for tokens
            token_data = self._exchange_code()
            self._store_tokens(token_data)
            return token_data

        # No tokens and no code - need authorization
        auth_url = self._get_auth_url()
        raise AuthException(
            f"Authorization required. Please visit the following URL to authorize:\n{auth_url}",
            auth_url=auth_url,
        )

    def check_connection(self) -> HandlerStatusResponse:
        """
        Check if the connection to Xero API is active

        Returns:
            HandlerStatusResponse: Status response with connection details
        """
        response = HandlerStatusResponse(success=False)

        try:
            self.connect()

            # Try to fetch identity information to verify connection
            identity_api = IdentityApi(self.api_client)
            connections = identity_api.get_connections()

            if len(connections) > 0:
                response.success = True
                # IMPORTANT: Set copy_storage to persist refreshed tokens between requests
                # This ensures that tokens refreshed during this connection are saved for future connections
                response.copy_storage = "success"
            else:
                response.error_message = "No Xero connections found for this user"
        except AuthException as e:
            # For auth exceptions, return them with redirect URL if available
            response.success = False
            response.error_message = str(e)
            if hasattr(e, 'auth_url') and e.auth_url:
                response.redirect_url = e.auth_url
        except Exception as e:
            response.error_message = f"Connection check failed: {str(e)}"

        return response

    def _setup_api_client(self, access_token: str) -> None:
        """
        Setup the Xero API client with the access token

        Args:
            access_token: OAuth2 access token
        """
        # Create OAuth2Token object with the access token
        oauth2_token = OAuth2Token(
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

        # Update token with the access token
        oauth2_token.update_token(
            access_token=access_token,
            refresh_token=None,
            scope=["openid", "profile", "email", "accounting.transactions", "accounting.settings"],
            expires_in=1800,  # 30 minutes
            token_type="Bearer",
        )

        # Create configuration with the OAuth2Token
        configuration = Configuration(oauth2_token=oauth2_token)

        # Create ApiClient with dummy token saver/getter (we're read-only)
        self.api_client = ApiClient(
            configuration=configuration,
            oauth2_token_saver=lambda token: None,  # No-op saver
            oauth2_token_getter=lambda: {
                "access_token": oauth2_token.access_token,
                "refresh_token": oauth2_token.refresh_token,
                "scope": oauth2_token.scope,
                "expires_in": oauth2_token.expires_in,
                "token_type": oauth2_token.token_type,
            },
        )

    def _get_auth_url(self) -> str:
        """
        Generate the Xero OAuth2 authorization URL

        Returns:
            str: Authorization URL for user to visit
        """
        base_url = "https://login.xero.com/identity/connect/authorize"
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "scope": (
                "openid profile email accounting.transactions accounting.settings offline_access"
            ),
            "state": "security_token",
        }

        query_string = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{base_url}?{query_string}"

    def _exchange_code(self) -> Dict[str, Any]:
        """
        Exchange authorization code for access and refresh tokens

        Returns:
            dict: Token data including access_token, refresh_token, expires_at, and tenant_id

        Raises:
            Exception: If code exchange fails
        """
        token_url = "https://identity.xero.com/connect/token"

        data = {
            "grant_type": "authorization_code",
            "code": self.code,
            "redirect_uri": self.redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response = requests.post(token_url, data=data)
        response.raise_for_status()

        token_response = response.json()

        # Get tenant ID from identity endpoint
        tenant_id = self._get_tenant_id(token_response["access_token"])

        return {
            "access_token": token_response["access_token"],
            "refresh_token": token_response["refresh_token"],
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=token_response["expires_in"]),
            "tenant_id": tenant_id,
        }

    def _refresh_tokens(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refresh the access token using the refresh token with Basic Authentication.

        Per Xero documentation, token refresh requires:
        - Basic Authentication header with base64(client_id:client_secret)
        - POST request body with grant_type and refresh_token

        **CRITICAL: Xero Rotating Refresh Tokens**
        - Xero invalidates the refresh token after each use
        - The response ALWAYS includes a new refresh_token
        - We MUST extract and use this new token, not fall back to the old one
        - Using the old token will fail with "Invalid refresh token" on next refresh

        Args:
            refresh_token: OAuth2 refresh token

        Returns:
            dict: Updated token data with access_token, NEW refresh_token, expires_at, tenant_id

        Raises:
            ValueError: If credentials are missing
            Exception: If token refresh fails or response doesn't contain new refresh token
        """
        token_url = "https://identity.xero.com/connect/token"

        # Validate that client credentials are available
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Client ID and Client Secret are required to refresh tokens. "
                "Please provide these credentials in your connection configuration."
            )

        # Create Basic Authentication header
        auth_string = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {auth_string}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        response = requests.post(token_url, headers=headers, data=data)
        response.raise_for_status()

        token_response = response.json()

        # CRITICAL: Xero ALWAYS returns a new refresh_token in the response
        # If it's missing, something went wrong
        if "refresh_token" not in token_response:
            raise Exception(
                f"Xero token refresh response did not include a new refresh_token. "
                f"This indicates a critical issue with token rotation. Response keys: {list(token_response.keys())}"
            )

        # Preserve tenant_id from connection data or stored tokens
        tenant_id = (
            self.connection_data.get("tenant_id")
            or (self._load_stored_tokens() or {}).get("tenant_id")
        )

        return {
            "access_token": token_response["access_token"],
            "refresh_token": token_response["refresh_token"],  # Use NEW refresh token, never fallback to old one
            "expires_at": datetime.now(timezone.utc) + timedelta(seconds=token_response["expires_in"]),
            "tenant_id": tenant_id,
        }

    def _get_tenant_id(self, access_token: str) -> str:
        """
        Get the tenant ID from Xero's identity endpoint

        Args:
            access_token: OAuth2 access token

        Returns:
            str: Tenant ID (organization ID)
        """
        identity_url = "https://api.xero.com/api.xro/2.0/Connections"
        headers = {"Authorization": f"Bearer {access_token}"}

        response = requests.get(identity_url, headers=headers)
        response.raise_for_status()

        connections = response.json()
        if connections and len(connections) > 0:
            return connections[0].get("tenantId")

        raise Exception("No Xero tenants found for this user")

    def _is_token_expired(self, token_data: Dict[str, Any]) -> bool:
        """
        Check if the access token is expired or about to expire.

        Tokens are considered expired if they expire within 5 minutes (grace period).
        Supports both ISO 8601 string format and Unix timestamps.

        Args:
            token_data: Token data dictionary

        Returns:
            bool: True if token is expired or expires within 5 minutes
        """
        if not token_data or "expires_at" not in token_data:
            return True

        expires_at = token_data["expires_at"]
        if not expires_at:
            return True

        # Parse expires_at to datetime
        if isinstance(expires_at, str):
            try:
                expires_at = datetime.fromisoformat(expires_at)
            except (ValueError, TypeError):
                # If parsing fails, assume expired
                return True
        elif isinstance(expires_at, (int, float)):
            # Unix timestamp
            expires_at = datetime.fromtimestamp(expires_at)
        else:
            # Unknown format, assume expired
            return True

        # Consider token expired if it expires within 5 minutes (grace period)
        buffer_time = datetime.now(timezone.utc) + timedelta(minutes=5)
        return buffer_time > expires_at

    def _store_tokens(self, token_data: Dict[str, Any]) -> None:
        """
        Store tokens securely in handler storage

        Args:
            token_data: Token data to store
        """
        if not self.handler_storage:
            return

        # Convert datetime to string for JSON serialization
        stored_data = token_data.copy()
        if isinstance(stored_data.get("expires_at"), datetime):
            stored_data["expires_at"] = stored_data["expires_at"].isoformat()

        self.handler_storage.encrypted_json_set("xero_tokens", stored_data)

    def _load_stored_tokens(self) -> Optional[Dict[str, Any]]:
        """
        Load stored tokens from handler storage

        Returns:
            dict: Stored token data or None if not found
        """
        if not self.handler_storage:
            return None

        try:
            token_data = self.handler_storage.encrypted_json_get("xero_tokens")
            if token_data and isinstance(token_data.get("expires_at"), str):
                token_data["expires_at"] = datetime.fromisoformat(token_data["expires_at"])
            return token_data
        except Exception:
            return None

    def native_query(self, query: str) -> None:
        """
        Execute native query - not supported for Xero

        Args:
            query: SQL query

        Raises:
            NotImplementedError
        """
        raise NotImplementedError("Native queries are not supported for Xero handler")

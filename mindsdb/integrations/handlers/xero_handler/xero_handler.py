import requests
from typing import Optional, Dict, Any
from datetime import datetime, timedelta
import json

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import HandlerStatusResponse
from mindsdb.integrations.utilities.handlers.auth_utilities.exceptions import AuthException

from xero_python.identity import IdentityApi
from xero_python.api_client import ApiClient, Configuration
from xero_python.accounting import AccountingApi

from .xero_tables import (
    BudgetsTable,
    ContactsTable,
    InvoicesTable,
    ItemsTable,
    OverpaymentsTable,
    PaymentsTable,
    PurchaseOrdersTable,
    QuotesTable,
    RepeatingInvoicesTable,
    AccountsTable,
)


class XeroHandler(APIHandler):
    """
    Xero Handler for MindsDB

    Implements OAuth2 authentication with Xero API and provides read-only access
    to accounting data including budgets, contacts, invoices, items, and more.
    """

    name = 'xero'

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
        self._register_table("budgets", BudgetsTable(self))
        self._register_table("contacts", ContactsTable(self))
        self._register_table("invoices", InvoicesTable(self))
        self._register_table("items", ItemsTable(self))
        self._register_table("overpayments", OverpaymentsTable(self))
        self._register_table("payments", PaymentsTable(self))
        self._register_table("purchase_orders", PurchaseOrdersTable(self))
        self._register_table("quotes", QuotesTable(self))
        self._register_table("repeating_invoices", RepeatingInvoicesTable(self))
        self._register_table("accounts", AccountsTable(self))

    def connect(self) -> ApiClient:
        """
        Establish connection to Xero API with OAuth2 authentication

        Handles the complete OAuth2 flow:
        1. Checks for existing stored tokens
        2. Refreshes token if expired
        3. Exchanges authorization code for tokens if provided
        4. Raises AuthException with authorization URL if no tokens available

        Returns:
            ApiClient: The configured Xero API client
        """
        if self.is_connected and self.connection is not None:
            return self.connection

        try:
            # Try to load existing tokens from storage
            token_data = self._load_stored_tokens()

            if token_data:
                # Check if token is expired
                if self._is_token_expired(token_data):
                    token_data = self._refresh_tokens(token_data["refresh_token"])
                    self._store_tokens(token_data)
            elif self.code:
                # Exchange authorization code for tokens
                token_data = self._exchange_code()
                self._store_tokens(token_data)
            else:
                # No tokens and no code - need authorization
                auth_url = self._get_auth_url()
                raise AuthException(
                    f"Authorization required. Please visit the following URL to authorize:\n{auth_url}",
                    auth_url=auth_url,
                )

            # Set tenant_id if provided or use stored one
            self.tenant_id = self.connection_data.get("tenant_id") or token_data.get(
                "tenant_id"
            )

            # Create API client with access token
            self._setup_api_client(token_data["access_token"])
            self.is_connected = True

        except AuthException:
            raise
        except Exception as e:
            raise Exception(f"Failed to connect to Xero: {str(e)}")

        return self.connection

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
        configuration = Configuration(
            access_token=access_token,
            api_key_prefix={"Authorization": "Bearer"},
        )
        self.api_client = ApiClient(configuration)

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
            "expires_at": datetime.now() + timedelta(seconds=token_response["expires_in"]),
            "tenant_id": tenant_id,
        }

    def _refresh_tokens(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refresh the access token using the refresh token

        Args:
            refresh_token: OAuth2 refresh token

        Returns:
            dict: Updated token data

        Raises:
            Exception: If token refresh fails
        """
        token_url = "https://identity.xero.com/connect/token"

        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response = requests.post(token_url, data=data)
        response.raise_for_status()

        token_response = response.json()

        # Preserve tenant_id
        stored_data = self._load_stored_tokens()
        tenant_id = stored_data.get("tenant_id") if stored_data else None

        return {
            "access_token": token_response["access_token"],
            "refresh_token": token_response["refresh_token"],
            "expires_at": datetime.now() + timedelta(seconds=token_response["expires_in"]),
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
        Check if the access token is expired or about to expire

        Args:
            token_data: Token data dictionary

        Returns:
            bool: True if token is expired or expires within 5 minutes
        """
        if "expires_at" not in token_data:
            return True

        expires_at = token_data["expires_at"]
        if isinstance(expires_at, str):
            expires_at = datetime.fromisoformat(expires_at)

        # Consider token expired if it expires within 5 minutes
        return datetime.now() > expires_at - timedelta(minutes=5)

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

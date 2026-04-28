from typing import Any

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.passthrough import PassthroughMixin
from mindsdb.integrations.libs.passthrough_types import PassthroughRequest
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log

logger = log.getLogger(__name__)


AUTH_TYPE_BEARER = "bearer"
AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS = "oauth_client_credentials"
SUPPORTED_AUTH_TYPES = (AUTH_TYPE_BEARER, AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS)

DEFAULT_AUTH_TYPE = AUTH_TYPE_BEARER
DEFAULT_TOKEN_AUTH_METHOD = "client_secret_post"
SUPPORTED_TOKEN_AUTH_METHODS = ("client_secret_post", "client_secret_basic")

# Fields that only make sense in OAuth mode. Bearer-mode configs that
# populate any of these are rejected so misconfigured datasources fail
# loudly at sync time rather than silently ignoring the OAuth values.
OAUTH_ONLY_FIELDS = (
    "token_url",
    "client_id",
    "client_secret",
    "scope",
    "audience",
    "token_auth_method",
)


class RestApiHandler(APIHandler, PassthroughMixin):
    """Generic REST API handler — passthrough only, no SQL tables.

    This is the "bring your own URL" escape hatch for any bearer-token API
    that mindsdb doesn't have a named handler for. Users supply a base_url
    and a bearer_token and get full passthrough access.
    """

    name = "rest_api"

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data") or {}
        self.kwargs = kwargs
        self.is_connected = False

        # PassthroughMixin reads these instance attributes at runtime.
        self._bearer_token_arg = "bearer_token"
        self._base_url_default = None  # user must supply base_url

        # Build the test request from connection_data. Default to GET /
        # unless the user provided a custom test_path.
        test_path = self.connection_data.get("test_path", "/")
        if not test_path.startswith("/"):
            test_path = f"/{test_path}"
        self._test_request = PassthroughRequest(method="GET", path=test_path)

    def _get_auth_type(self) -> str:
        """Return the configured auth_type, defaulting to 'bearer'.

        Empty strings are treated as "not set" so a UI that submits empty
        form fields still defaults correctly.
        """
        value = self.connection_data.get("auth_type")
        if value is None or value == "":
            return DEFAULT_AUTH_TYPE
        return str(value)

    def _validate_auth_config(self) -> None:
        """Validate stored auth-related connection args.

        Runs against the *stored* datasource config (synced from Minds), not
        runtime passthrough requests. Raises ValueError on misconfiguration.
        """
        auth_type = self._get_auth_type()
        if auth_type not in SUPPORTED_AUTH_TYPES:
            raise ValueError(
                f"Unsupported auth_type '{auth_type}'. Supported values: {', '.join(SUPPORTED_AUTH_TYPES)}"
            )

        if auth_type == AUTH_TYPE_BEARER:
            self._validate_bearer_config()
        else:
            self._validate_oauth_client_credentials_config()

    def _validate_bearer_config(self) -> None:
        if not self.connection_data.get("bearer_token"):
            raise ValueError("bearer_token is required when auth_type is 'bearer'")

        present_oauth_fields = [field for field in OAUTH_ONLY_FIELDS if self.connection_data.get(field)]
        if present_oauth_fields:
            raise ValueError(
                f"OAuth-only fields are not permitted when auth_type is 'bearer': {', '.join(present_oauth_fields)}"
            )

    def _validate_oauth_client_credentials_config(self) -> None:
        if self.connection_data.get("bearer_token"):
            raise ValueError("bearer_token is not permitted when auth_type is 'oauth_client_credentials'")

        for required in ("token_url", "client_id", "client_secret"):
            if not self.connection_data.get(required):
                raise ValueError(f"{required} is required when auth_type is 'oauth_client_credentials'")

        method = self.connection_data.get("token_auth_method") or DEFAULT_TOKEN_AUTH_METHOD
        if method not in SUPPORTED_TOKEN_AUTH_METHODS:
            raise ValueError(
                f"Unsupported token_auth_method '{method}'. Supported values: {', '.join(SUPPORTED_TOKEN_AUTH_METHODS)}"
            )

    def connect(self) -> None:
        """No persistent connection needed — passthrough is stateless.

        Validation happens in check_connection(), which we
        call separately during CREATE DATABASE.
        """
        self.is_connected = True

    def check_connection(self) -> StatusResponse:
        """Validate that base_url and the configured auth strategy are valid."""
        response = StatusResponse(False)
        try:
            base_url = self._build_base_url()
            if not base_url:
                response.error_message = "base_url is required"
                return response
            self._validate_auth_config()
            response.success = True
            self.is_connected = True
        except Exception as e:
            response.error_message = str(e)
        return response

    def native_query(self, query: str) -> Response:
        """Not supported — use passthrough instead."""
        return Response(
            RESPONSE_TYPE.ERROR,
            error_message="rest_api handler is passthrough-only. Use the /passthrough endpoint.",
        )

    def get_tables(self) -> Response:
        """No SQL tables — passthrough only."""
        import pandas as pd

        return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

    def get_columns(self, table_name: str) -> Response:
        """No SQL tables — passthrough only."""
        return Response(
            RESPONSE_TYPE.ERROR,
            error_message="rest_api handler is passthrough-only. No tables available.",
        )

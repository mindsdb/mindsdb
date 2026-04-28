from typing import Any, Optional

from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.passthrough import PassthroughMixin
from mindsdb.integrations.libs.passthrough_types import (
    PassthroughConfigError,
    PassthroughRequest,
    PassthroughResponse,
)
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
    OAuth2ClientCredentialsProvider,
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

    # Advertised to /capabilities. Both modes are supported by the same
    # handler instance — the runtime mode is selected per-datasource via
    # connection_data["auth_type"]. _auth_mode is kept as a fallback for
    # any caller that still reads the single-mode field.
    _auth_modes = ["bearer", "oauth_client_credentials"]
    _auth_mode = "bearer"

    def __init__(self, name: str, **kwargs: Any) -> None:
        super().__init__(name)
        self.connection_data = kwargs.get("connection_data") or {}
        self.kwargs = kwargs
        self.handler_storage = kwargs.get("handler_storage")
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

        # Lazy: instantiated on first token fetch when auth_type is OAuth.
        # Bearer-mode handlers never construct a provider.
        self._oauth_provider: Optional[OAuth2ClientCredentialsProvider] = None

    def _oauth_storage_key(self) -> str:
        """Storage key for the OAuth token cache.

        Namespaced by handler name so two datasources with different client_ids
        on the same MindsDB instance never share a cached token.
        """
        return f"oauth_client_credentials_tokens:{self.name}"

    def _maybe_init_oauth_provider(self) -> None:
        """Idempotent. Builds the OAuth provider only in oauth_client_credentials mode.

        Bearer-mode handlers never reach this path, so the provider is never
        constructed for them — that's the contract enforced by the
        bearer_mode_does_not_instantiate_oauth_provider test.
        """
        if self._oauth_provider is not None:
            return
        if self._get_auth_type() != AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS:
            return
        self._oauth_provider = OAuth2ClientCredentialsProvider(
            self.connection_data,
            handler_storage=self.handler_storage,
            storage_key=self._oauth_storage_key(),
        )

    def _get_bearer_token(self) -> str:
        """Token resolution dispatcher used by PassthroughMixin.

        - bearer mode: keep the mixin's default (read from connection_data).
        - oauth_client_credentials: resolve via the OAuth provider, which
          handles token fetch, caching, and refresh.

        Caller-supplied Authorization headers are filtered out by
        PassthroughMixin._build_outgoing_headers (Authorization is in
        FORBIDDEN_REQUEST_HEADERS) before this method's return value is
        written into the outgoing headers, so the generated auth always wins.
        """
        auth_type = self._get_auth_type()
        if auth_type == AUTH_TYPE_BEARER:
            return super()._get_bearer_token()
        if auth_type == AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS:
            self._maybe_init_oauth_provider()
            assert self._oauth_provider is not None  # _maybe_init guarantees this
            return self._oauth_provider.get_access_token()
        raise PassthroughConfigError(f"Unsupported auth_type '{auth_type}'")

    def _secrets_for_scrub(self) -> list[str]:
        """Per-request list of values to redact from the upstream response.

        Differs from PassthroughMixin's default in two ways:
        1. In OAuth mode it uses the provider's `current_secrets()` (which
           returns the *currently cached* access token, or `[]` when uncached)
           instead of `_get_bearer_token()` — the latter would force a token
           fetch purely to populate the scrub list, which is wasteful and
           could fail (e.g. IdP unreachable) in a code path that's only
           supposed to redact strings from the response body.
        2. The list is deduplicated and stripped of empties before return so
           the underlying string-replace loop never does redundant work.

        The default-headers heuristic (treat values >= 16 chars as potential
        secrets) is preserved verbatim so handlers that move from bearer to
        OAuth without changing default_headers see the same scrubbing.
        """
        secrets: list[str] = []
        auth_type = self._get_auth_type()

        if auth_type == AUTH_TYPE_BEARER:
            token = self.connection_data.get("bearer_token")
            if token:
                secrets.append(str(token))
        elif auth_type == AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS:
            # Only the currently-cached access token, never client_secret.
            # current_secrets() returns [] before the first successful token
            # fetch, so calling _secrets_for_scrub early is safe.
            if self._oauth_provider is not None:
                secrets.extend(self._oauth_provider.current_secrets())

        defaults = self.connection_data.get("default_headers") or {}
        if isinstance(defaults, dict):
            for value in defaults.values():
                s = str(value)
                if len(s) >= 16:
                    secrets.append(s)

        seen: set[str] = set()
        deduped: list[str] = []
        for s in secrets:
            if s and s not in seen:
                seen.add(s)
                deduped.append(s)
        return deduped

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
        """Validate that base_url and the configured auth strategy are valid.

        Bearer mode: schema-only validation (backward-compatible).

        OAuth mode: schema validation, then construct the provider (which
        runs the SSRF check on token_url), fetch a token to confirm the IdP
        accepts the credentials, then call test_passthrough() to confirm the
        upstream accepts the token. Errors are reported as plain strings —
        the underlying layers (provider error sanitization, test_passthrough's
        structured result) already redact secrets, so we forward their
        messages without re-formatting.
        """
        response = StatusResponse(False)
        try:
            base_url = self._build_base_url()
            if not base_url:
                response.error_message = "base_url is required"
                return response
            self._validate_auth_config()

            if self._get_auth_type() == AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS:
                # Surface IdP / SSRF / token-shape problems at connect time.
                self._maybe_init_oauth_provider()
                assert self._oauth_provider is not None
                self._oauth_provider.get_access_token()

                # _test_request is always set in __init__ (defaults to GET /),
                # so the upstream sanity check always runs in OAuth mode. Its
                # return is a structured dict with a safe `message` field.
                test_result = self.test_passthrough()
                if not test_result.get("ok"):
                    response.error_message = (
                        test_result.get("message") or test_result.get("error_code") or "upstream test request failed"
                    )
                    return response

            response.success = True
            self.is_connected = True
        except Exception as e:
            response.error_message = str(e)
        return response

    def api_passthrough(self, req: PassthroughRequest) -> PassthroughResponse:
        """Forward to PassthroughMixin, with a single 401 retry in OAuth mode.

        On the first 401 from the upstream, assume the cached access token
        was rejected (revoked, rotated by the IdP, or invalidated mid-flight),
        clear the token cache, and replay the request once. The replay goes
        through the full mixin path again — same SSRF / allowed_hosts checks,
        same Authorization-header override protection, same response scrub —
        so a second 401 is returned to the caller as-is rather than triggering
        another retry. Bearer mode is untouched.
        """
        response = super().api_passthrough(req)
        if (
            response.status_code == 401
            and self._get_auth_type() == AUTH_TYPE_OAUTH_CLIENT_CREDENTIALS
            and self._oauth_provider is not None
        ):
            self._oauth_provider.clear_cached_token()
            response = super().api_passthrough(req)
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

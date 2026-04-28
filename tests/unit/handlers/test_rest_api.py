"""Unit tests for the generic REST API passthrough handler."""

from unittest.mock import patch, MagicMock

from mindsdb.integrations.handlers.rest_api_handler import (
    connection_args as exported_connection_args,
)
from mindsdb.integrations.handlers.rest_api_handler.connection_args import connection_args
from mindsdb.integrations.handlers.rest_api_handler.oauth_connection_args import (
    oauth_connection_args,
)
from mindsdb.integrations.handlers.rest_api_handler.rest_connection_args import (
    rest_connection_args,
)
from mindsdb.integrations.handlers.rest_api_handler.rest_api_handler import RestApiHandler
from mindsdb.integrations.libs.const import HANDLER_CONNECTION_ARG_TYPE as ARG_TYPE
from mindsdb.integrations.libs.passthrough import PassthroughProtocol
from mindsdb.integrations.libs.passthrough_types import PassthroughRequest, PassthroughResponse
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
)


VALID_DATA = {
    "base_url": "https://api.example.com",
    "bearer_token": "test-token-123",
}


def _make_handler(connection_data=None):
    if connection_data is None:
        connection_data = dict(VALID_DATA)
    return RestApiHandler("test_rest", connection_data=connection_data)


# ---------------------------------------------------------------------------
# Shared stub helpers for OAuth tests
#
# Hoisted to the top so any test class can use them. The helpers monkeypatch
# the OAuth provider's DNS / token-endpoint POST, and the passthrough mixin's
# upstream `requests.request` call.
# ---------------------------------------------------------------------------


def _stub_oauth_dns(monkeypatch):
    """Stub socket.getaddrinfo so the provider's SSRF check passes for hostnames."""
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        client_credentials as cc_module,
    )

    def fake_getaddrinfo(host, *args, **kwargs):
        return [(2, 1, 6, "", ("1.2.3.4", 0))]

    monkeypatch.setattr(cc_module.socket, "getaddrinfo", fake_getaddrinfo)


def _stub_token_endpoint(monkeypatch, access_token="OAUTH-AT", expires_in=3600):
    """Patch the token POST to return a synthetic access token.

    Returns a counter dict with key 'n' incremented on each call, so tests
    can assert how many times the IdP was hit.
    """
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        client_credentials as cc_module,
    )

    calls = {"n": 0}

    def fake_post(url, data=None, headers=None, **_):
        calls["n"] += 1

        class _Resp:
            status_code = 200
            is_redirect = False
            headers = {}

            def iter_content(self, chunk_size=4096):
                import json as _json

                payload = _json.dumps({"access_token": access_token, "expires_in": expires_in}).encode("utf-8")
                yield payload

            def close(self):
                pass

        return _Resp()

    monkeypatch.setattr(cc_module.requests, "post", fake_post)
    return calls


def _stub_upstream(monkeypatch):
    """Patch the upstream request so we can inspect the outgoing call."""
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers", {})

        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {}
        resp.iter_content.return_value = [b""]
        resp.close = MagicMock()
        return resp

    from mindsdb.integrations.libs import passthrough as pt_module

    monkeypatch.setattr(pt_module.requests, "request", fake_request)
    return captured


class TestRestApiHandlerInit:
    def test_satisfies_passthrough_protocol(self):
        assert issubclass(RestApiHandler, PassthroughProtocol)

    def test_stores_connection_data(self):
        data = {"base_url": "https://x.com", "bearer_token": "tok"}
        handler = _make_handler(data)
        assert handler.connection_data == data

    def test_default_test_request_path(self):
        handler = _make_handler()
        assert handler._test_request.method == "GET"
        assert handler._test_request.path == "/"

    def test_custom_test_path(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "test_path": "/health",
            }
        )
        assert handler._test_request.path == "/health"

    def test_custom_test_path_without_slash(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "test_path": "status",
            }
        )
        assert handler._test_request.path == "/status"


class TestCheckConnection:
    def test_success(self):
        handler = _make_handler()
        response = handler.check_connection()
        assert isinstance(response, StatusResponse)
        assert response.success is True
        assert not response.error_message

    def test_missing_base_url(self):
        handler = _make_handler({"bearer_token": "tok"})
        response = handler.check_connection()
        assert response.success is False
        assert "base_url" in response.error_message

    def test_missing_bearer_token(self):
        handler = _make_handler({"base_url": "https://api.example.com"})
        response = handler.check_connection()
        assert response.success is False
        assert "bearer_token" in response.error_message

    def test_empty_connection_data(self):
        handler = _make_handler({})
        response = handler.check_connection()
        assert response.success is False


class TestPassthroughIntegration:
    """Test that the mixin methods work correctly on RestApiHandler."""

    @patch("mindsdb.integrations.libs.passthrough.requests.request")
    def test_api_passthrough_injects_bearer(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.iter_content.return_value = [b'{"ok": true}']
        mock_resp.close = MagicMock()
        mock_request.return_value = mock_resp

        handler = _make_handler()
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/v1/users"))

        assert isinstance(result, PassthroughResponse)
        assert result.status_code == 200
        headers = mock_request.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer test-token-123"

    @patch("mindsdb.integrations.libs.passthrough.requests.request")
    def test_api_passthrough_uses_base_url(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_resp.iter_content.return_value = [b""]
        mock_resp.close = MagicMock()
        mock_request.return_value = mock_resp

        handler = _make_handler()
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        called_url = mock_request.call_args.args[1]
        assert called_url == "https://api.example.com/foo"

    @patch("mindsdb.integrations.libs.passthrough.requests.request")
    def test_api_passthrough_includes_default_headers(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        mock_resp.iter_content.return_value = [b""]
        mock_resp.close = MagicMock()
        mock_request.return_value = mock_resp

        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "default_headers": {"Accept": "application/json", "X-Team": "data"},
            }
        )
        handler.api_passthrough(PassthroughRequest(method="GET", path="/"))

        headers = mock_request.call_args.kwargs["headers"]
        assert headers["Accept"] == "application/json"
        assert headers["X-Team"] == "data"

    @patch("mindsdb.integrations.libs.passthrough.requests.request")
    def test_test_passthrough_success(self, mock_request):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"Content-Type": "application/json"}
        mock_resp.iter_content.return_value = [b'{"ok": true}']
        mock_resp.close = MagicMock()
        mock_request.return_value = mock_resp

        handler = _make_handler()
        result = handler.test_passthrough()

        assert isinstance(result, dict)
        assert result["ok"] is True
        assert result["status_code"] == 200

    def test_test_passthrough_with_no_network(self):
        """test_passthrough catches connection errors gracefully."""
        handler = _make_handler()
        result = handler.test_passthrough()
        assert isinstance(result, dict)
        assert result["ok"] is False
        assert result["error_code"] in ("network", "unknown")


class TestConnectionArgsSchema:
    """The exported connection_args is the union of REST + auth modules."""

    def test_rest_module_only_holds_passthrough_fields(self):
        assert set(rest_connection_args.keys()) == {
            "base_url",
            "default_headers",
            "allowed_hosts",
            "test_path",
        }

    def test_auth_module_only_holds_auth_fields(self):
        assert set(oauth_connection_args.keys()) == {
            "auth_type",
            "bearer_token",
            "token_url",
            "client_id",
            "client_secret",
            "scope",
            "audience",
            "token_auth_method",
        }

    def test_bearer_token_lives_in_auth_module(self):
        # bearer_token is an auth strategy, not a REST/passthrough setting.
        assert "bearer_token" not in rest_connection_args
        assert "bearer_token" in oauth_connection_args

    def test_aggregated_connection_args_includes_rest_fields(self):
        for key in ("base_url", "default_headers", "allowed_hosts", "test_path"):
            assert key in connection_args

    def test_aggregated_connection_args_includes_bearer_token(self):
        assert "bearer_token" in connection_args

    def test_aggregated_connection_args_includes_oauth_fields(self):
        for key in (
            "auth_type",
            "token_url",
            "client_id",
            "client_secret",
            "scope",
            "audience",
            "token_auth_method",
        ):
            assert key in connection_args

    def test_client_secret_marked_secret_and_pwd(self):
        spec = connection_args["client_secret"]
        assert spec["type"] == ARG_TYPE.PWD
        assert spec.get("secret") is True

    def test_bearer_token_marked_secret_and_pwd(self):
        spec = connection_args["bearer_token"]
        assert spec["type"] == ARG_TYPE.PWD
        assert spec.get("secret") is True

    def test_package_exports_aggregated_args(self):
        # The handler package re-exports connection_args; make sure the
        # aggregator and the package-level export are the same object.
        assert exported_connection_args is connection_args


class TestBackwardCompatibleBearerInit:
    """Existing bearer-only configs must still initialize and validate."""

    def test_legacy_config_initializes(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "legacy-token",
            }
        )
        assert handler.connection_data["base_url"] == "https://api.example.com"
        assert handler.connection_data["bearer_token"] == "legacy-token"

    def test_legacy_config_check_connection_succeeds(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "legacy-token",
            }
        )
        response = handler.check_connection()
        assert response.success is True


class TestAuthTypeResolution:
    def test_default_auth_type_is_bearer(self):
        handler = _make_handler()
        assert handler._get_auth_type() == "bearer"

    def test_empty_auth_type_treated_as_default(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "auth_type": "",
            }
        )
        assert handler._get_auth_type() == "bearer"

    def test_explicit_auth_type_returned(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "auth_type": "oauth_client_credentials",
                "token_url": "https://auth.example.com/token",
                "client_id": "cid",
                "client_secret": "csecret",
            }
        )
        assert handler._get_auth_type() == "oauth_client_credentials"


class TestBearerAuthValidation:
    def test_implicit_bearer_with_token_passes(self):
        handler = _make_handler({"base_url": "https://api.example.com", "bearer_token": "tok"})
        handler._validate_auth_config()  # should not raise

    def test_explicit_bearer_with_token_passes(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "auth_type": "bearer",
            }
        )
        handler._validate_auth_config()

    def test_missing_bearer_token_fails(self):
        handler = _make_handler({"base_url": "https://api.example.com", "auth_type": "bearer"})
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "bearer_token" in str(e)
        else:
            raise AssertionError("expected ValueError for missing bearer_token")

    def test_check_connection_missing_bearer_token_message(self):
        handler = _make_handler({"base_url": "https://api.example.com", "auth_type": "bearer"})
        response = handler.check_connection()
        assert response.success is False
        assert "bearer_token" in response.error_message

    def test_bearer_rejects_token_url(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "token_url": "https://auth.example.com/token",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "token_url" in str(e)
        else:
            raise AssertionError("expected ValueError for token_url in bearer mode")

    def test_bearer_rejects_client_id(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "client_id": "cid",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "client_id" in str(e)
        else:
            raise AssertionError("expected ValueError for client_id in bearer mode")

    def test_bearer_rejects_client_secret(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "client_secret": "secret",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "client_secret" in str(e)
        else:
            raise AssertionError("expected ValueError for client_secret in bearer mode")

    def test_bearer_rejects_scope(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "scope": "read:all",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "scope" in str(e)
        else:
            raise AssertionError("expected ValueError for scope in bearer mode")

    def test_bearer_rejects_audience(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "audience": "https://api.example.com",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "audience" in str(e)
        else:
            raise AssertionError("expected ValueError for audience in bearer mode")

    def test_bearer_rejects_token_auth_method(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "token_auth_method": "client_secret_post",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "token_auth_method" in str(e)
        else:
            raise AssertionError("expected ValueError for token_auth_method in bearer mode")

    def test_bearer_ignores_empty_oauth_fields(self):
        # UIs may submit empty strings for unfilled fields; treat as absent.
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok",
                "token_url": "",
                "client_id": "",
                "client_secret": "",
            }
        )
        handler._validate_auth_config()


class TestOAuthClientCredentialsValidation:
    BASE = {
        "base_url": "https://api.example.com",
        "auth_type": "oauth_client_credentials",
        "token_url": "https://auth.example.com/token",
        "client_id": "cid",
        "client_secret": "csecret",
    }

    def test_minimal_valid_config_passes(self):
        handler = _make_handler(dict(self.BASE))
        handler._validate_auth_config()

    def test_missing_token_url_fails(self):
        data = dict(self.BASE)
        del data["token_url"]
        handler = _make_handler(data)
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "token_url" in str(e)
        else:
            raise AssertionError("expected ValueError for missing token_url")

    def test_missing_client_id_fails(self):
        data = dict(self.BASE)
        del data["client_id"]
        handler = _make_handler(data)
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "client_id" in str(e)
        else:
            raise AssertionError("expected ValueError for missing client_id")

    def test_missing_client_secret_fails(self):
        data = dict(self.BASE)
        del data["client_secret"]
        handler = _make_handler(data)
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "client_secret" in str(e)
        else:
            raise AssertionError("expected ValueError for missing client_secret")

    def test_oauth_rejects_bearer_token(self):
        data = dict(self.BASE)
        data["bearer_token"] = "tok"
        handler = _make_handler(data)
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "bearer_token" in str(e)
        else:
            raise AssertionError("expected ValueError for bearer_token in OAuth mode")

    def test_oauth_default_token_auth_method_passes(self):
        # token_auth_method omitted → defaults to client_secret_post.
        handler = _make_handler(dict(self.BASE))
        handler._validate_auth_config()

    def test_oauth_explicit_client_secret_basic_passes(self):
        data = dict(self.BASE)
        data["token_auth_method"] = "client_secret_basic"
        handler = _make_handler(data)
        handler._validate_auth_config()

    def test_oauth_unsupported_token_auth_method_fails(self):
        data = dict(self.BASE)
        data["token_auth_method"] = "private_key_jwt"
        handler = _make_handler(data)
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "token_auth_method" in str(e)
            assert "private_key_jwt" in str(e)
        else:
            raise AssertionError("expected ValueError for unsupported token_auth_method")

    def test_oauth_check_connection_succeeds(self, monkeypatch):
        # OAuth check_connection now actually fetches a token and runs the
        # upstream test request, so we need to stub DNS, the token endpoint,
        # and the upstream HTTP call.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.BASE))
        response = handler.check_connection()
        assert response.success is True


class TestUnsupportedAuthType:
    def test_unsupported_auth_type_fails(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "auth_type": "api_key",
                "bearer_token": "tok",
            }
        )
        try:
            handler._validate_auth_config()
        except ValueError as e:
            assert "auth_type" in str(e)
            assert "api_key" in str(e)
        else:
            raise AssertionError("expected ValueError for unsupported auth_type")

    def test_unsupported_auth_type_via_check_connection(self):
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "auth_type": "saml",
            }
        )
        response = handler.check_connection()
        assert response.success is False
        assert "auth_type" in response.error_message


# ---------------------------------------------------------------------------
# OAuth client credentials integration
# ---------------------------------------------------------------------------


class TestOAuthIntegration:
    OAUTH_CONFIG = {
        "base_url": "https://api.example.com",
        "auth_type": "oauth_client_credentials",
        "token_url": "https://auth.example.com/token",
        "client_id": "cid",
        "client_secret": "csec",
    }

    def test_oauth_mode_fetches_token_via_provider(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        token_calls = _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/v1/users"))

        assert token_calls["n"] == 1
        assert handler._oauth_provider is not None
        assert captured["headers"]["Authorization"] == "Bearer OAUTH-AT"

    def test_oauth_token_cached_across_calls(self, monkeypatch):
        # Two passthrough calls, one token fetch — proves we go through the
        # provider's cache rather than the static-token path.
        _stub_oauth_dns(monkeypatch)
        token_calls = _stub_token_endpoint(monkeypatch)
        _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/v1/users"))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/v1/orgs"))

        assert token_calls["n"] == 1

    def test_oauth_caller_authorization_cannot_override_generated_auth(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(
            PassthroughRequest(
                method="GET",
                path="/v1/users",
                headers={"Authorization": "Bearer attacker-token"},
            )
        )

        assert captured["headers"]["Authorization"] == "Bearer OAUTH-AT"
        assert "attacker-token" not in captured["headers"]["Authorization"]

    def test_oauth_caller_authorization_lowercase_also_rejected(self, monkeypatch):
        # FORBIDDEN_REQUEST_HEADERS check is case-insensitive; verify a
        # lowercase header from the caller still loses to the generated auth.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(
            PassthroughRequest(
                method="GET",
                path="/foo",
                headers={"authorization": "Bearer attacker-token"},
            )
        )

        # The mixin always writes the canonical-cased "Authorization" header.
        assert captured["headers"]["Authorization"] == "Bearer OAUTH-AT"
        # The caller-supplied lowercase variant should not have leaked through.
        # If anything's in headers under "authorization" lowercase, it must
        # not be the attacker token.
        if "authorization" in captured["headers"]:
            assert "attacker-token" not in captured["headers"]["authorization"]

    def test_oauth_passthrough_works_without_caller_auth_headers(self, monkeypatch):
        # Anton-style call: no headers at all on the PassthroughRequest.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/v1/users"))

        assert captured["headers"]["Authorization"] == "Bearer OAUTH-AT"

    def test_bearer_mode_does_not_instantiate_oauth_provider(self, monkeypatch):
        _stub_upstream(monkeypatch)

        handler = _make_handler()  # bearer mode (default)
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert handler._oauth_provider is None

    def test_implicit_bearer_mode_still_injects_static_token(self, monkeypatch):
        # Config without auth_type → defaults to bearer → static token used.
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler({"base_url": "https://api.example.com", "bearer_token": "static-tok"})
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert captured["headers"]["Authorization"] == "Bearer static-tok"
        assert handler._oauth_provider is None

    def test_explicit_bearer_mode_still_injects_static_token(self, monkeypatch):
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "auth_type": "bearer",
                "bearer_token": "static-tok",
            }
        )
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert captured["headers"]["Authorization"] == "Bearer static-tok"
        assert handler._oauth_provider is None

    def test_bearer_caller_authorization_cannot_override_generated_auth(self, monkeypatch):
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler({"base_url": "https://api.example.com", "bearer_token": "static-tok"})
        handler.api_passthrough(
            PassthroughRequest(
                method="GET",
                path="/foo",
                headers={"Authorization": "Bearer attacker-token"},
            )
        )

        assert captured["headers"]["Authorization"] == "Bearer static-tok"
        assert "attacker-token" not in captured["headers"]["Authorization"]

    def test_oauth_storage_key_includes_handler_name(self, monkeypatch):
        # Two providers built for two different handler names must use
        # distinct storage keys, so token caches don't collide on a shared
        # handler_storage instance.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        _stub_upstream(monkeypatch)

        h1 = RestApiHandler("ds_one", connection_data=dict(self.OAUTH_CONFIG))
        h2 = RestApiHandler("ds_two", connection_data=dict(self.OAUTH_CONFIG))
        h1.api_passthrough(PassthroughRequest(method="GET", path="/foo"))
        h2.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert h1._oauth_provider.storage_key != h2._oauth_provider.storage_key
        assert "ds_one" in h1._oauth_provider.storage_key
        assert "ds_two" in h2._oauth_provider.storage_key

    def test_oauth_provider_receives_handler_storage(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        _stub_upstream(monkeypatch)

        sentinel_storage = MagicMock()
        sentinel_storage.encrypted_json_get.return_value = None
        handler = RestApiHandler(
            "test_rest",
            connection_data=dict(self.OAUTH_CONFIG),
            handler_storage=sentinel_storage,
        )
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert handler._oauth_provider.handler_storage is sentinel_storage


# ---------------------------------------------------------------------------
# Response scrubbing
# ---------------------------------------------------------------------------


REDACTED = "[REDACTED_API_KEY]"


def _stub_upstream_with_body(monkeypatch, body_bytes, content_type="text/plain"):
    """Patch the upstream so its response body contains caller-controlled bytes.

    Returns the captured-headers dict (populated on each call) so tests can
    confirm what was sent in addition to what came back.
    """
    captured = {}

    def fake_request(method, url, **kwargs):
        captured["headers"] = kwargs.get("headers", {})
        resp = MagicMock()
        resp.status_code = 200
        resp.headers = {"Content-Type": content_type}
        # `body_bytes` may be a callable so each request can return different
        # bytes (e.g., echoing whatever token was sent).
        payload = body_bytes(captured) if callable(body_bytes) else body_bytes
        resp.iter_content.return_value = [payload]
        resp.close = MagicMock()
        return resp

    from mindsdb.integrations.libs import passthrough as pt_module

    monkeypatch.setattr(pt_module.requests, "request", fake_request)
    return captured


class TestResponseScrubbing:
    """Tokens — static or rotating — must never reach the runtime caller."""

    OAUTH_CONFIG = {
        "base_url": "https://api.example.com",
        "auth_type": "oauth_client_credentials",
        "token_url": "https://auth.example.com/token",
        "client_id": "cid",
        "client_secret": "csec",
    }

    def test_static_bearer_scrubbed_when_upstream_echoes_it(self, monkeypatch):
        _stub_upstream_with_body(monkeypatch, b"upstream said: test-token-123 hi")

        handler = _make_handler()
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert "test-token-123" not in str(result.body)
        assert REDACTED in str(result.body)

    def test_oauth_token_scrubbed_when_upstream_echoes_it(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="OAUTH-AT")
        _stub_upstream_with_body(monkeypatch, b"hi OAUTH-AT here is your data")

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert "OAUTH-AT" not in str(result.body)
        assert REDACTED in str(result.body)

    def test_runtime_caller_never_receives_oauth_access_token(self, monkeypatch):
        # Belt-and-suspenders for the spec's "runtime caller never receives
        # OAuth access token if upstream echoes it" — exercises the full
        # api_passthrough path including JSON decoding.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="SECRET-AT-9000")
        _stub_upstream_with_body(
            monkeypatch,
            b'{"echoed_token": "SECRET-AT-9000", "ok": true}',
            content_type="application/json",
        )

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        # Body parsed as JSON; serialize it back to verify scrub holds across
        # the JSON round-trip.
        import json as _json

        rendered = _json.dumps(result.body)
        assert "SECRET-AT-9000" not in rendered
        assert REDACTED in rendered

    def test_rotated_oauth_token_scrubbed(self, monkeypatch):
        # Force the provider to issue a different token after invalidation
        # (the manual rotation path). The scrub list should track the
        # currently-cached token, not stale ones.
        _stub_oauth_dns(monkeypatch)

        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )

        token_iter = iter(["TOKEN-A", "TOKEN-B"])

        def fake_post(*a, **kw):
            class _R:
                status_code = 200
                is_redirect = False
                headers = {}

                def iter_content(self, chunk_size=4096):
                    import json as _j

                    yield _j.dumps({"access_token": next(token_iter), "expires_in": 3600}).encode()

                def close(self):
                    pass

            return _R()

        monkeypatch.setattr(cc_module.requests, "post", fake_post)

        # Upstream echoes whichever token was sent.
        def body_for(captured):
            sent = captured["headers"]["Authorization"].replace("Bearer ", "")
            return f"echo {sent}".encode()

        _stub_upstream_with_body(monkeypatch, body_for)

        handler = _make_handler(dict(self.OAUTH_CONFIG))

        # First call — uses TOKEN-A.
        r1 = handler.api_passthrough(PassthroughRequest(method="GET", path="/a"))
        assert "TOKEN-A" not in str(r1.body)
        assert REDACTED in str(r1.body)

        # Rotate.
        handler._oauth_provider.clear_cached_token()

        # Second call — uses TOKEN-B; scrub list reflects the new token.
        r2 = handler.api_passthrough(PassthroughRequest(method="GET", path="/b"))
        assert "TOKEN-B" not in str(r2.body)
        assert REDACTED in str(r2.body)

    def test_empty_secrets_dropped_from_scrub_list(self):
        # bearer_token is the empty string (e.g. UI submitted blank); the
        # scrub list must not contain "" because str.replace("", X) inserts
        # the sentinel between every character of the response body.
        handler = _make_handler({"base_url": "https://api.example.com", "bearer_token": ""})
        secrets = handler._secrets_for_scrub()
        assert "" not in secrets

    def test_duplicate_secrets_deduplicated(self):
        # bearer_token equals a default_headers value — both would otherwise
        # be appended to the scrub list. The override dedupes.
        long_value = "shared-long-value-1234567890"  # ≥ 16 chars
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": long_value,
                "default_headers": {"X-Token": long_value},
            }
        )
        secrets = handler._secrets_for_scrub()
        assert secrets.count(long_value) == 1

    def test_oauth_current_secrets_does_not_trigger_token_fetch(self, monkeypatch):
        # _secrets_for_scrub must not POST to the IdP just to populate its
        # list. current_secrets() returns [] when uncached.
        _stub_oauth_dns(monkeypatch)
        token_calls = _stub_token_endpoint(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler._maybe_init_oauth_provider()

        secrets = handler._secrets_for_scrub()

        assert token_calls["n"] == 0
        assert secrets == []

    def test_bearer_mode_scrub_list_unchanged_with_default_headers(self):
        # Pre-existing bearer behavior: the bearer token plus any
        # default_headers values >= 16 chars. Order doesn't matter for
        # correctness, but membership does.
        long_value = "x" * 32
        short_value = "shortie"
        handler = _make_handler(
            {
                "base_url": "https://api.example.com",
                "bearer_token": "tok-12345",
                "default_headers": {"X-Long": long_value, "X-Short": short_value},
            }
        )
        secrets = handler._secrets_for_scrub()
        assert "tok-12345" in secrets
        assert long_value in secrets
        assert short_value not in secrets  # too short to be treated as secret

    def test_oauth_mode_does_not_include_static_bearer_token(self, monkeypatch):
        # In OAuth mode, even if a stale bearer_token field somehow survived
        # in connection_data (it shouldn't — _validate_auth_config rejects
        # it), the scrub override skips reading it. Belt-and-suspenders to
        # ensure the static field doesn't sneak into the scrub list.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="LIVE-AT")
        _stub_upstream(monkeypatch)

        cfg = dict(self.OAUTH_CONFIG)
        # Bypass validation to construct the test scenario.
        handler = RestApiHandler("test_rest", connection_data=cfg)
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        secrets = handler._secrets_for_scrub()
        assert "LIVE-AT" in secrets
        assert handler.connection_data.get("bearer_token") in (None, "")

    def test_oauth_mode_scrub_does_not_leak_client_secret(self, monkeypatch):
        # client_secret must never appear in the scrub list (it shouldn't be
        # in responses either, but if it ever were, we don't want a redaction
        # path that accidentally implies it's tracked as a "current secret").
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="LIVE-AT")
        _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        secrets = handler._secrets_for_scrub()
        assert "csec" not in secrets
        assert "cid" not in secrets


# ---------------------------------------------------------------------------
# OAuth 401 retry
# ---------------------------------------------------------------------------


def _make_response(status_code=200, body=b"", headers=None):
    """Build a minimal MagicMock standing in for requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.iter_content.return_value = [body]
    resp.close = MagicMock()
    return resp


def _stub_upstream_with_status_sequence(monkeypatch, statuses, body_for_status=None):
    """Patch upstream to return a different status code on each successive call.

    `statuses` is a list of integers; each call pops the next one. After the
    list is exhausted, the last status is repeated.
    `body_for_status` (optional callable) returns bytes given (call_idx, status,
    captured_headers) so a test can shape the body per call.
    """
    state = {"calls": [], "idx": 0}

    def fake_request(method, url, **kwargs):
        idx = state["idx"]
        status = statuses[min(idx, len(statuses) - 1)]
        state["idx"] += 1
        captured = {
            "method": method,
            "url": url,
            "headers": dict(kwargs.get("headers", {})),
        }
        state["calls"].append(captured)
        if body_for_status is not None:
            body = body_for_status(idx, status, captured["headers"])
        else:
            body = b""
        return _make_response(status_code=status, body=body)

    from mindsdb.integrations.libs import passthrough as pt_module

    monkeypatch.setattr(pt_module.requests, "request", fake_request)
    return state


class TestOAuthRetryOn401:
    OAUTH_CONFIG = {
        "base_url": "https://api.example.com",
        "auth_type": "oauth_client_credentials",
        "token_url": "https://auth.example.com/token",
        "client_id": "cid",
        "client_secret": "csec",
    }

    def test_401_clears_cached_token_then_refetches(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)

        # Token endpoint returns a different token on each call so we can
        # observe the cache having been cleared.
        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )

        token_iter = iter(["TOKEN-A", "TOKEN-B"])
        token_calls = {"n": 0}

        def fake_post(*a, **kw):
            token_calls["n"] += 1

            class _R:
                status_code = 200
                is_redirect = False
                headers = {}

                def iter_content(self, chunk_size=4096):
                    import json as _j

                    yield _j.dumps({"access_token": next(token_iter), "expires_in": 3600}).encode()

                def close(self):
                    pass

            return _R()

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401, 200])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        # Two upstream calls (401 then 200) and two token POSTs (initial fetch
        # then post-clear refetch).
        assert len(upstream["calls"]) == 2
        assert token_calls["n"] == 2
        assert result.status_code == 200

    def test_retry_uses_new_token(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)

        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )

        token_iter = iter(["TOKEN-A", "TOKEN-B"])

        def fake_post(*a, **kw):
            class _R:
                status_code = 200
                is_redirect = False
                headers = {}

                def iter_content(self, chunk_size=4096):
                    import json as _j

                    yield _j.dumps({"access_token": next(token_iter), "expires_in": 3600}).encode()

                def close(self):
                    pass

            return _R()

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401, 200])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert upstream["calls"][0]["headers"]["Authorization"] == "Bearer TOKEN-A"
        assert upstream["calls"][1]["headers"]["Authorization"] == "Bearer TOKEN-B"

    def test_retried_response_scrubs_new_token(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)

        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )

        token_iter = iter(["TOKEN-A", "TOKEN-B"])

        def fake_post(*a, **kw):
            class _R:
                status_code = 200
                is_redirect = False
                headers = {}

                def iter_content(self, chunk_size=4096):
                    import json as _j

                    yield _j.dumps({"access_token": next(token_iter), "expires_in": 3600}).encode()

                def close(self):
                    pass

            return _R()

        monkeypatch.setattr(cc_module.requests, "post", fake_post)

        # First call: 401 with body echoing TOKEN-A. Second call: 200 with
        # body echoing TOKEN-B. Final response should redact TOKEN-B (the
        # "new" token), since current_secrets() reflects the post-retry cache.
        def body_for(idx, status, headers):
            sent = headers["Authorization"].replace("Bearer ", "")
            return f"echoed: {sent}".encode()

        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401, 200], body_for_status=body_for)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert result.status_code == 200
        assert "TOKEN-B" not in str(result.body)
        assert "[REDACTED_API_KEY]" in str(result.body)
        assert len(upstream["calls"]) == 2

    def test_repeated_401_does_not_loop(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        token_calls = _stub_token_endpoint(monkeypatch)
        # Upstream returns 401 every time.
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        # Exactly one retry, so two upstream calls and two token POSTs.
        # The second 401 is returned to the caller as-is.
        assert len(upstream["calls"]) == 2
        assert token_calls["n"] == 2
        assert result.status_code == 401

    def test_bearer_mode_does_not_retry_on_401(self, monkeypatch):
        # Bearer mode: a 401 from the upstream is returned as-is. No second
        # request, no retry logic.
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401])

        handler = _make_handler()  # default bearer
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        assert len(upstream["calls"]) == 1
        assert result.status_code == 401

    def test_non_401_status_does_not_retry(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [403])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        result = handler.api_passthrough(PassthroughRequest(method="GET", path="/foo"))

        # 403 (or any non-401) does not trigger the retry path.
        assert len(upstream["calls"]) == 1
        assert result.status_code == 403


# ---------------------------------------------------------------------------
# OAuth check_connection
# ---------------------------------------------------------------------------


class TestOAuthCheckConnection:
    OAUTH_CONFIG = {
        "base_url": "https://api.example.com",
        "auth_type": "oauth_client_credentials",
        "token_url": "https://auth.example.com/token",
        "client_id": "cid",
        "client_secret": "csec",
    }

    def test_oauth_check_connection_fetches_token(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        token_calls = _stub_token_endpoint(monkeypatch)
        _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is True
        assert token_calls["n"] == 1

    def test_oauth_check_connection_calls_test_path(self, monkeypatch):
        # Default test_path is "/", so the upstream sanity request goes to
        # base_url + "/".
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is True
        assert captured["url"].startswith("https://api.example.com")
        assert captured["url"].endswith("/")

    def test_oauth_check_connection_calls_custom_test_path(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        captured = _stub_upstream(monkeypatch)

        cfg = dict(self.OAUTH_CONFIG)
        cfg["test_path"] = "/healthz"
        handler = _make_handler(cfg)
        response = handler.check_connection()

        assert response.success is True
        assert captured["url"] == "https://api.example.com/healthz"

    def test_oauth_check_connection_token_endpoint_failure_is_safe(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        # Token endpoint returns 401.
        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )

        def fake_post(*a, **kw):
            class _R:
                status_code = 401
                is_redirect = False
                headers = {}

                def iter_content(self, chunk_size=4096):
                    import json as _j

                    yield _j.dumps({"error": "invalid_client"}).encode()

                def close(self):
                    pass

            return _R()

        monkeypatch.setattr(cc_module.requests, "post", fake_post)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is False
        # client_secret must not leak into the error message.
        assert "csec" not in (response.error_message or "")
        # The upstream of the IdP error should be reflected back to the caller
        # in some form (status code reference is fine).
        assert response.error_message  # non-empty

    def test_oauth_check_connection_upstream_test_failure_is_safe(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="OAUTH-AT")
        # Upstream returns 500 — token was fetched OK but the upstream itself
        # rejected the request.
        _stub_upstream_with_status_sequence(monkeypatch, [500])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is False
        assert "OAUTH-AT" not in (response.error_message or "")
        assert "csec" not in (response.error_message or "")

    def test_oauth_check_connection_401_then_success_via_retry(self, monkeypatch):
        # The upstream test goes through api_passthrough, so the 401-retry
        # applies during check_connection too: a 401 + subsequent 200 makes
        # check_connection succeed.
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch)
        upstream = _stub_upstream_with_status_sequence(monkeypatch, [401, 200])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is True
        assert len(upstream["calls"]) == 2

    def test_oauth_check_connection_persistent_401_fails_safely(self, monkeypatch):
        _stub_oauth_dns(monkeypatch)
        _stub_token_endpoint(monkeypatch, access_token="OAUTH-AT")
        _stub_upstream_with_status_sequence(monkeypatch, [401])

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is False
        # test_passthrough's 401 message is "upstream rejected credentials..."
        # which is safe — it doesn't include the token or client_secret.
        assert "OAUTH-AT" not in (response.error_message or "")
        assert "csec" not in (response.error_message or "")

    def test_bearer_check_connection_does_not_make_http_calls(self, monkeypatch):
        # Bearer mode keeps schema-only check_connection. If anything reaches
        # the network, this test will catch it.
        called = {"n": 0}

        def boom(*a, **kw):
            called["n"] += 1
            raise AssertionError("bearer check_connection must not hit the network")

        from mindsdb.integrations.libs import passthrough as pt_module

        monkeypatch.setattr(pt_module.requests, "request", boom)

        handler = _make_handler()
        response = handler.check_connection()

        assert response.success is True
        assert called["n"] == 0

    def test_bearer_check_connection_missing_token_unchanged(self):
        # Pre-existing bearer error path is preserved.
        handler = _make_handler({"base_url": "https://api.example.com"})
        response = handler.check_connection()
        assert response.success is False
        assert "bearer_token" in response.error_message

    def test_oauth_check_connection_no_secrets_in_error(self, monkeypatch):
        # Provoke a connection error at the IdP transport layer and check
        # that neither client_secret nor anything resembling an Authorization
        # header value leaks into response.error_message.
        _stub_oauth_dns(monkeypatch)

        from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
            client_credentials as cc_module,
        )
        import requests as _requests

        def boom(*a, **kw):
            raise _requests.ConnectionError(
                f"transport failure carrying client_secret={self.OAUTH_CONFIG['client_secret']}"
            )

        monkeypatch.setattr(cc_module.requests, "post", boom)

        handler = _make_handler(dict(self.OAUTH_CONFIG))
        response = handler.check_connection()

        assert response.success is False
        # The provider's _sanitize_exception strips client_secret from
        # transport-error chains; verify the final surface is clean.
        msg = response.error_message or ""
        assert self.OAUTH_CONFIG["client_secret"] not in msg
        assert "Bearer " not in msg

    def test_oauth_check_connection_unsupported_auth_type_unchanged(self):
        # Pre-existing path — unsupported auth_type still errors out via the
        # schema validator without touching the network.
        handler = _make_handler({"base_url": "https://api.example.com", "auth_type": "saml"})
        response = handler.check_connection()
        assert response.success is False
        assert "auth_type" in response.error_message

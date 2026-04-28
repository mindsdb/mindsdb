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

    def test_oauth_check_connection_succeeds(self):
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

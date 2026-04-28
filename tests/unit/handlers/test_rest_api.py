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

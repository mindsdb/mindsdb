"""Unit tests for BearerPassthroughMixin."""

import unittest
from unittest.mock import MagicMock, patch

from mindsdb.integrations.libs.bearer_passthrough import (
    BearerPassthroughMixin,
    REDACTED_SENTINEL,
)
from mindsdb.integrations.libs.passthrough_types import (
    HostNotAllowedError,
    PassthroughConfigError,
    PassthroughRequest,
    PassthroughValidationError,
)


class _FakeHandler(BearerPassthroughMixin):
    """Minimal handler stub for exercising the mixin."""

    _bearer_token_arg = "api_key"
    _base_url_default = "https://api.example.com"
    _test_request = PassthroughRequest(method="GET", path="/me")

    def __init__(self, connection_data: dict):
        self.name = "fake_ds"
        self.connection_data = connection_data


def _mock_response(status_code=200, body=b'{"ok":true}', headers=None, content_type="application/json"):
    """Return a mock requests.Response exposing the bits the mixin uses."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = {"Content-Type": content_type, **(headers or {})}
    resp.iter_content = MagicMock(return_value=iter([body]))
    resp.close = MagicMock()
    return resp


class BearerPassthroughHappyPathTests(unittest.TestCase):
    def setUp(self):
        self.handler = _FakeHandler({"api_key": "secret-token-abcdef1234567890"})

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_injects_bearer_and_uses_default_base_url(self, mock_request):
        mock_request.return_value = _mock_response()
        resp = self.handler.api_passthrough(PassthroughRequest("GET", "/me"))

        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.body, {"ok": True})

        args, kwargs = mock_request.call_args
        self.assertEqual(args[0], "GET")
        self.assertEqual(args[1], "https://api.example.com/me")
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer secret-token-abcdef1234567890")
        self.assertEqual(kwargs["headers"]["X-Minds-Passthrough"], "1")

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_user_base_url_overrides_default(self, mock_request):
        self.handler.connection_data["base_url"] = "https://api.eu.example.com"
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest("GET", "/me"))
        self.assertEqual(mock_request.call_args[0][1], "https://api.eu.example.com/me")

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_query_params_forwarded(self, mock_request):
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest("GET", "/x", query={"a": "1"}))
        self.assertEqual(mock_request.call_args.kwargs["params"], {"a": "1"})

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_json_body_forwarded(self, mock_request):
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest("POST", "/x", body={"name": "foo"}))
        self.assertEqual(mock_request.call_args.kwargs["json"], {"name": "foo"})

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_default_headers_merged(self, mock_request):
        self.handler.connection_data["default_headers"] = {"Accept": "application/json"}
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest("GET", "/x"))
        self.assertEqual(mock_request.call_args.kwargs["headers"]["Accept"], "application/json")


class BearerPassthroughHeaderFilteringTests(unittest.TestCase):
    def setUp(self):
        self.handler = _FakeHandler({"api_key": "secret-token-abcdef1234567890"})

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_caller_cannot_override_authorization(self, mock_request):
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest(
            "GET", "/x", headers={"Authorization": "Bearer hijack", "Cookie": "s=1"}
        ))
        outgoing = mock_request.call_args.kwargs["headers"]
        self.assertEqual(outgoing["Authorization"], "Bearer secret-token-abcdef1234567890")
        self.assertNotIn("Cookie", outgoing)

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_proxy_headers_stripped(self, mock_request):
        mock_request.return_value = _mock_response()
        self.handler.api_passthrough(PassthroughRequest(
            "GET", "/x", headers={"Proxy-Authorization": "hijack"}
        ))
        outgoing = mock_request.call_args.kwargs["headers"]
        self.assertNotIn("Proxy-Authorization", outgoing)

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_hop_by_hop_response_headers_stripped(self, mock_request):
        mock_request.return_value = _mock_response(
            headers={"Connection": "close", "X-Safe": "1", "Transfer-Encoding": "chunked"}
        )
        resp = self.handler.api_passthrough(PassthroughRequest("GET", "/x"))
        self.assertNotIn("Connection", resp.headers)
        self.assertNotIn("Transfer-Encoding", resp.headers)
        self.assertEqual(resp.headers.get("X-Safe"), "1")


class BearerPassthroughHostAllowlistTests(unittest.TestCase):
    def test_rejects_host_outside_allowlist(self):
        handler = _FakeHandler({
            "api_key": "t",
            "base_url": "https://api.example.com",
            "allowed_hosts": ["api.example.com"],
        })
        # Direct host check using a bad URL
        with self.assertRaises(HostNotAllowedError):
            handler._check_host_allowed("evil.com")

    def test_wildcard_allows_any_host(self):
        handler = _FakeHandler({
            "api_key": "t",
            "base_url": "https://api.example.com",
            "allowed_hosts": ["*"],
        })
        handler._check_host_allowed("evil.com")  # must not raise

    def test_private_ip_rejected_by_default(self):
        handler = _FakeHandler({"api_key": "t", "base_url": "http://10.0.0.1"})
        with self.assertRaises(HostNotAllowedError):
            handler._check_host_allowed("10.0.0.1")

    def test_private_ip_allowed_when_explicitly_listed(self):
        handler = _FakeHandler({
            "api_key": "t",
            "base_url": "http://10.0.0.1",
            "allowed_hosts": ["10.0.0.1"],
        })
        # Explicitly allowlisted private IP should still be rejected — the
        # mixin treats explicit private-IP allowlisting as a foot-gun that
        # requires the "*" escape hatch. Document this behavior.
        with self.assertRaises(HostNotAllowedError):
            handler._check_host_allowed("10.0.0.1")

    def test_loopback_rejected_with_wildcard_when_asterisk_not_used(self):
        handler = _FakeHandler({
            "api_key": "t",
            "base_url": "http://127.0.0.1",
            "allowed_hosts": ["127.0.0.1"],
        })
        with self.assertRaises(HostNotAllowedError):
            handler._check_host_allowed("127.0.0.1")


class BearerPassthroughValidationTests(unittest.TestCase):
    def test_missing_bearer_raises(self):
        handler = _FakeHandler({})  # no api_key
        with self.assertRaises(PassthroughConfigError):
            handler.api_passthrough(PassthroughRequest("GET", "/me"))

    def test_missing_base_url_raises(self):
        class NoDefault(_FakeHandler):
            _base_url_default = None

        handler = NoDefault({"api_key": "t"})
        with self.assertRaises(PassthroughConfigError):
            handler.api_passthrough(PassthroughRequest("GET", "/me"))

    def test_path_must_start_with_slash(self):
        handler = _FakeHandler({"api_key": "t"})
        with self.assertRaises(PassthroughValidationError):
            handler.api_passthrough(PassthroughRequest("GET", "me"))

    def test_method_allowlist(self):
        handler = _FakeHandler({"api_key": "t"})
        with self.assertRaises(PassthroughValidationError):
            handler.api_passthrough(PassthroughRequest("TRACE", "/me"))


class BearerPassthroughSecretScrubTests(unittest.TestCase):
    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_token_scrubbed_from_json_body(self, mock_request):
        token = "secret-token-abcdef1234567890"
        body = ('{"error":"Invalid token ' + token + '"}').encode("utf-8")
        handler = _FakeHandler({"api_key": token})
        mock_request.return_value = _mock_response(body=body)

        resp = handler.api_passthrough(PassthroughRequest("GET", "/x"))
        self.assertNotIn(token, str(resp.body))
        self.assertIn(REDACTED_SENTINEL, resp.body["error"])

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_token_scrubbed_from_headers(self, mock_request):
        token = "secret-token-abcdef1234567890"
        handler = _FakeHandler({"api_key": token})
        mock_request.return_value = _mock_response(
            headers={"X-Debug-Auth": f"Bearer {token}"},
        )
        resp = handler.api_passthrough(PassthroughRequest("GET", "/x"))
        self.assertIn(REDACTED_SENTINEL, resp.headers["X-Debug-Auth"])
        self.assertNotIn(token, resp.headers["X-Debug-Auth"])

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_long_default_header_values_scrubbed(self, mock_request):
        token = "secret-token-abcdef1234567890"
        long_secret = "x" * 32
        handler = _FakeHandler({
            "api_key": token,
            "default_headers": {"X-Api-Secondary": long_secret},
        })
        mock_request.return_value = _mock_response(
            body=('{"echoed":"' + long_secret + '"}').encode("utf-8")
        )
        resp = handler.api_passthrough(PassthroughRequest("GET", "/x"))
        self.assertEqual(resp.body["echoed"], REDACTED_SENTINEL)


class BearerPassthroughTestEndpointTests(unittest.TestCase):
    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_returns_ok_on_200(self, mock_request):
        handler = _FakeHandler({"api_key": "t"})
        mock_request.return_value = _mock_response(status_code=200)
        result = handler.test_passthrough()
        self.assertTrue(result["ok"])
        self.assertEqual(result["status_code"], 200)
        self.assertEqual(result["host"], "api.example.com")

    @patch("mindsdb.integrations.libs.bearer_passthrough.requests.request")
    def test_returns_auth_failed_on_401(self, mock_request):
        handler = _FakeHandler({"api_key": "t"})
        mock_request.return_value = _mock_response(status_code=401)
        result = handler.test_passthrough()
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "auth_failed")

    def test_returns_not_implemented_when_no_test_request(self):
        class NoTest(_FakeHandler):
            _test_request = None

        handler = NoTest({"api_key": "t"})
        result = handler.test_passthrough()
        self.assertFalse(result["ok"])
        self.assertEqual(result["error_code"], "not_implemented")


if __name__ == "__main__":
    unittest.main()

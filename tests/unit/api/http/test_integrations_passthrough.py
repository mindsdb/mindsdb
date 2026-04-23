"""HTTP-layer tests for the /api/integrations/<name>/passthrough routes.

Exercises the Flask blueprint in isolation: the session's integration
controller is mocked to return handlers with (or without) the
BearerPassthroughMixin, so these tests do not touch real handlers and
do not make network calls.
"""

from http import HTTPStatus
from unittest.mock import MagicMock, patch

from mindsdb.integrations.libs.bearer_passthrough import BearerPassthroughMixin
from mindsdb.integrations.libs.passthrough_types import PassthroughResponse


class _StubPassthroughHandler(BearerPassthroughMixin):
    """Handler double: the HTTP layer checks isinstance(..., mixin), then
    calls `api_passthrough`. We bypass all mixin internals by overriding
    `api_passthrough` directly so the endpoint test does not depend on
    connection_data, base_url resolution, or the requests library."""

    def __init__(self, response: PassthroughResponse):
        self._response = response
        self.calls: list = []

    def api_passthrough(self, req):  # type: ignore[override]
        self.calls.append(req)
        return self._response

    def test_passthrough(self):
        return {"ok": True, "status_code": self._response.status_code}


def _patch_handler(handler):
    """Patch FakeMysqlProxy so the endpoint resolves `name` to `handler`."""
    proxy = MagicMock()
    proxy.session.integration_controller.get_data_handler.return_value = handler
    return patch(
        "mindsdb.api.http.namespaces.integrations.FakeMysqlProxy",
        return_value=proxy,
    )


def test_passthrough_happy_path_returns_200_and_serialized_body(client):
    handler = _StubPassthroughHandler(
        PassthroughResponse(
            status_code=200,
            headers={"X-Safe": "1"},
            body={"hello": "world"},
            content_type="application/json",
        )
    )

    with _patch_handler(handler):
        response = client.post(
            "/api/integrations/any_ds/passthrough",
            json={"method": "GET", "path": "/me"},
        )

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload == {
        "status_code": 200,
        "headers": {"X-Safe": "1"},
        "body": {"hello": "world"},
        "content_type": "application/json",
    }
    # Request actually reached the mixin with the parsed PassthroughRequest.
    assert len(handler.calls) == 1
    assert handler.calls[0].method == "GET"
    assert handler.calls[0].path == "/me"


def test_passthrough_returns_501_when_handler_does_not_support_mixin(client):
    # A bare object is not a BearerPassthroughMixin, so the endpoint should
    # surface passthrough_not_supported (501) instead of a 500.
    with _patch_handler(object()):
        response = client.post(
            "/api/integrations/mysql/passthrough",
            json={"method": "GET", "path": "/anything"},
        )

    assert response.status_code == HTTPStatus.NOT_IMPLEMENTED
    payload = response.get_json()
    assert payload["error_code"] == "passthrough_not_supported"
    assert "mysql" in payload["message"]


def test_passthrough_returns_400_on_invalid_method(client):
    handler = _StubPassthroughHandler(PassthroughResponse(status_code=200, headers={}, body=None, content_type=None))

    with _patch_handler(handler):
        response = client.post(
            "/api/integrations/any_ds/passthrough",
            json={"method": "TRACE", "path": "/me"},
        )

    assert response.status_code == HTTPStatus.BAD_REQUEST
    payload = response.get_json()
    assert payload["error_code"] == "invalid_request"
    # The handler must not have been invoked when validation fails up front.
    assert handler.calls == []


def test_capabilities_returns_list_of_bearer_engines(client):
    # Build two fake handler modules: one opts in, one does not.
    class _OptedIn(BearerPassthroughMixin):
        pass

    class _NotOptedIn:
        pass

    opted_in_mod = MagicMock()
    opted_in_mod.Handler = _OptedIn
    plain_mod = MagicMock()
    plain_mod.Handler = _NotOptedIn
    no_handler_mod = MagicMock(spec=[])  # lacks a Handler attribute entirely

    fake_modules = {
        "strapi": opted_in_mod,
        "mysql": plain_mod,
        "broken": no_handler_mod,
    }

    with patch(
        "mindsdb.api.http.namespaces.integrations.integration_controller.handler_modules",
        fake_modules,
        create=True,
    ):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload == {"bearer_passthrough": ["strapi"]}

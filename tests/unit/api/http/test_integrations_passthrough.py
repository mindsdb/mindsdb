"""HTTP-layer tests for the /api/integrations/<name>/passthrough routes.

Exercises the Flask blueprint in isolation: the session's integration
controller is mocked to return handlers that satisfy
PassthroughProtocol, so these tests do not touch real handlers and do
not make network calls.
"""

from http import HTTPStatus
from unittest.mock import MagicMock, patch

from mindsdb.integrations.libs.passthrough import PassthroughMixin
from mindsdb.integrations.libs.passthrough_types import PassthroughResponse


class _StubPassthroughHandler(PassthroughMixin):
    """Handler double: the HTTP layer checks the PassthroughProtocol, then
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
    # A bare object does not satisfy PassthroughProtocol, so the endpoint
    # should surface passthrough_not_supported (501) instead of a 500.
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


def _patch_handler_modules(modules: dict):
    return patch(
        "mindsdb.api.http.namespaces.integrations.integration_controller.handler_modules",
        modules,
        create=True,
    )


def test_capabilities_returns_handlers_dict_and_legacy_list(client):
    # Two opted-in handlers covering both auth modes, one non-opt-in, and
    # one broken module that lacks a Handler attribute. auth_modes is
    # surfaced from the handler's declarative `_auth_mode` class attr —
    # not inferred from header format.
    class _BearerHandler(PassthroughMixin):
        pass  # inherits _auth_mode = "bearer"

    class _CustomHeaderHandler(PassthroughMixin):
        _auth_header_name = "X-Shopify-Access-Token"
        _auth_header_format = "{token}"
        _auth_mode = "custom"

    class _NotOptedIn:
        pass

    bearer_mod = MagicMock()
    bearer_mod.Handler = _BearerHandler
    custom_mod = MagicMock()
    custom_mod.Handler = _CustomHeaderHandler
    plain_mod = MagicMock()
    plain_mod.Handler = _NotOptedIn
    no_handler_mod = MagicMock(spec=[])

    fake_modules = {
        "hubspot": bearer_mod,
        "shopify": custom_mod,
        "mysql": plain_mod,
        "broken": no_handler_mod,
    }

    with _patch_handler_modules(fake_modules):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()

    # New structured shape: every opted-in handler appears with auth_modes
    # and operations metadata.
    assert payload["handlers"] == {
        "hubspot": {"auth_modes": ["bearer"], "operations": ["passthrough"]},
        "shopify": {"auth_modes": ["custom"], "operations": ["passthrough"]},
    }

    # Legacy flat list: only bearer-auth handlers (Minds migration compat).
    assert payload["bearer_passthrough"] == ["hubspot"]


def test_capabilities_auth_mode_is_declarative_not_format_derived(client):
    # Handler keeps the default "Bearer {token}" header format but flags
    # itself as oauth_refresh. The old format-matching heuristic would
    # have bucketed this as "bearer"; the new declarative path returns
    # the explicit mode and correctly omits it from the legacy list.
    class _OAuthRefreshHandler(PassthroughMixin):
        _auth_mode = "oauth_refresh"
        # _auth_header_format intentionally left as the default.

    oauth_mod = MagicMock()
    oauth_mod.Handler = _OAuthRefreshHandler

    with _patch_handler_modules({"hubspot_oauth": oauth_mod}):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload["handlers"] == {
        "hubspot_oauth": {"auth_modes": ["oauth_refresh"], "operations": ["passthrough"]},
    }
    # oauth_refresh is NOT surfaced in the legacy bearer-only list even
    # though the underlying header format is still "Bearer {token}".
    assert payload["bearer_passthrough"] == []


def test_capabilities_empty_when_no_handlers_opted_in(client):
    class _NotOptedIn:
        pass

    plain_mod = MagicMock()
    plain_mod.Handler = _NotOptedIn

    with _patch_handler_modules({"mysql": plain_mod}):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload == {"handlers": {}, "bearer_passthrough": []}


# ---------------------------------------------------------------------------
# Multi-mode (`_auth_modes`) capability resolution
# ---------------------------------------------------------------------------


def test_capabilities_handler_with_only_auth_mode_string_unchanged(client):
    # A handler that declares only the legacy `_auth_mode = "bearer"`
    # continues to surface as auth_modes: ["bearer"]. This pins the
    # backward-compat path of the new resolver.
    class _LegacyBearerHandler(PassthroughMixin):
        pass  # _auth_mode defaults to "bearer" via the mixin

    mod = MagicMock()
    mod.Handler = _LegacyBearerHandler

    with _patch_handler_modules({"legacy": mod}):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload["handlers"] == {
        "legacy": {"auth_modes": ["bearer"], "operations": ["passthrough"]},
    }
    assert payload["bearer_passthrough"] == ["legacy"]


def test_capabilities_handler_with_auth_modes_list_returns_all_modes(client):
    # The rest_api shape: a single handler advertising both bearer and
    # oauth_client_credentials. The endpoint must return both modes
    # verbatim and include the handler in `bearer_passthrough` because
    # "bearer" is among them.
    class _MultiAuthHandler(PassthroughMixin):
        _auth_modes = ["bearer", "oauth_client_credentials"]

    mod = MagicMock()
    mod.Handler = _MultiAuthHandler

    with _patch_handler_modules({"rest_api": mod}):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    assert payload["handlers"] == {
        "rest_api": {
            "auth_modes": ["bearer", "oauth_client_credentials"],
            "operations": ["passthrough"],
        },
    }
    assert payload["bearer_passthrough"] == ["rest_api"]


def test_capabilities_auth_modes_takes_precedence_over_auth_mode(client):
    # When both fields are declared (as on RestApiHandler — _auth_modes for
    # the new shape, _auth_mode kept as a fallback), the list wins. This is
    # the contract the resolver promises.
    class _DualDeclaredHandler(PassthroughMixin):
        _auth_modes = ["bearer", "oauth_client_credentials"]
        _auth_mode = "bearer"

    mod = MagicMock()
    mod.Handler = _DualDeclaredHandler

    with _patch_handler_modules({"rest_api": mod}):
        response = client.get("/api/integrations/capabilities")

    payload = response.get_json()
    assert payload["handlers"]["rest_api"]["auth_modes"] == [
        "bearer",
        "oauth_client_credentials",
    ]


def test_capabilities_bearer_passthrough_membership_per_auth_modes(client):
    # `bearer_passthrough` is populated based on whether "bearer" appears
    # in the resolved auth_modes, not on the legacy `_auth_mode` field.
    class _BearerOnly(PassthroughMixin):
        _auth_modes = ["bearer"]

    class _OAuthOnly(PassthroughMixin):
        _auth_modes = ["oauth_client_credentials"]

    class _Multi(PassthroughMixin):
        _auth_modes = ["bearer", "oauth_client_credentials"]

    bearer_mod = MagicMock()
    bearer_mod.Handler = _BearerOnly
    oauth_mod = MagicMock()
    oauth_mod.Handler = _OAuthOnly
    multi_mod = MagicMock()
    multi_mod.Handler = _Multi

    with _patch_handler_modules({"a_bearer": bearer_mod, "b_oauth": oauth_mod, "c_multi": multi_mod}):
        response = client.get("/api/integrations/capabilities")

    payload = response.get_json()
    assert payload["bearer_passthrough"] == ["a_bearer", "c_multi"]
    assert "b_oauth" not in payload["bearer_passthrough"]


def test_capabilities_handler_with_empty_auth_modes_falls_back_to_auth_mode(client):
    # An empty list is treated as "not declared" and the resolver falls
    # back to `_auth_mode`, which itself falls back to "bearer".
    class _EmptyListHandler(PassthroughMixin):
        _auth_modes = []
        _auth_mode = "custom"

    mod = MagicMock()
    mod.Handler = _EmptyListHandler

    with _patch_handler_modules({"odd": mod}):
        response = client.get("/api/integrations/capabilities")

    payload = response.get_json()
    assert payload["handlers"]["odd"]["auth_modes"] == ["custom"]


def test_capabilities_endpoint_stable_when_handler_module_is_broken(client):
    # If reading attrs off one module raises, the endpoint must still
    # return 200 with the other handlers intact.
    class _Healthy(PassthroughMixin):
        _auth_modes = ["bearer"]

    healthy_mod = MagicMock()
    healthy_mod.Handler = _Healthy

    # An "evil" module whose Handler attribute access raises. Use a
    # PropertyMock-style trick: set Handler to a property that raises.
    class _ExplodingModule:
        @property
        def Handler(self):  # noqa: N802 — matches handler-module API
            raise RuntimeError("module import side-effect blew up")

    broken_mod = _ExplodingModule()

    with _patch_handler_modules({"healthy": healthy_mod, "broken": broken_mod}):
        response = client.get("/api/integrations/capabilities")

    assert response.status_code == HTTPStatus.OK
    payload = response.get_json()
    # Healthy handler still surfaces; broken one is silently skipped.
    assert payload["handlers"] == {
        "healthy": {"auth_modes": ["bearer"], "operations": ["passthrough"]},
    }
    assert payload["bearer_passthrough"] == ["healthy"]


# ---------------------------------------------------------------------------
# Direct unit tests for the resolver helper
# ---------------------------------------------------------------------------


def test_resolve_auth_modes_prefers_list_over_string():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    class H:
        _auth_modes = ["bearer", "oauth_client_credentials"]
        _auth_mode = "bearer"

    assert _resolve_auth_modes(H) == ["bearer", "oauth_client_credentials"]


def test_resolve_auth_modes_falls_back_to_auth_mode_string():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    class H:
        _auth_mode = "custom"

    assert _resolve_auth_modes(H) == ["custom"]


def test_resolve_auth_modes_defaults_to_bearer_when_nothing_declared():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    class H:
        pass

    assert _resolve_auth_modes(H) == ["bearer"]


def test_resolve_auth_modes_handles_none_handler_class():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    assert _resolve_auth_modes(None) == ["bearer"]


def test_resolve_auth_modes_normalizes_tuple_to_list_of_strings():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    class H:
        _auth_modes = ("bearer", "oauth_client_credentials")

    result = _resolve_auth_modes(H)
    assert result == ["bearer", "oauth_client_credentials"]
    assert isinstance(result, list)


def test_resolve_auth_modes_skips_empty_list_and_falls_through():
    from mindsdb.api.http.namespaces.integrations import _resolve_auth_modes

    class H:
        _auth_modes = []
        _auth_mode = "custom"

    assert _resolve_auth_modes(H) == ["custom"]

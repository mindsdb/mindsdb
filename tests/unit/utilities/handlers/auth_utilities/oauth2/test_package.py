"""Sanity tests for the auth_utilities.oauth2 package surface.

These tests intentionally do not exercise behavior — they verify only that
the public import path, provider class, and exposed constants exist as the
rest of the codebase expects to import them.
"""

from __future__ import annotations


def test_provider_imports_from_package_root():
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        OAuth2ClientCredentialsProvider,
    )

    assert isinstance(OAuth2ClientCredentialsProvider, type)


def test_provider_exposes_required_methods():
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        OAuth2ClientCredentialsProvider,
    )

    for method in ("get_access_token", "clear_cached_token", "current_secrets"):
        assert callable(getattr(OAuth2ClientCredentialsProvider, method, None)), (
            f"OAuth2ClientCredentialsProvider is missing public method {method!r}"
        )


def test_constructor_accepts_connection_data_keyword():
    import inspect
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        OAuth2ClientCredentialsProvider,
    )

    sig = inspect.signature(OAuth2ClientCredentialsProvider.__init__)
    params = sig.parameters
    assert "connection_data" in params
    assert "handler_storage" in params
    assert "storage_key" in params
    # handler_storage and storage_key default to None so the provider can be
    # constructed without persistent storage.
    assert params["handler_storage"].default is None
    assert params["storage_key"].default is None


def test_public_constants_exposed_at_package_root():
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        ALLOWED_AUTH_METHODS,
        CONNECT_TIMEOUT_SECONDS,
        DEFAULT_EXPIRES_IN_SECONDS,
        DEFAULT_STORAGE_KEY,
        DEFAULT_TOKEN_AUTH_METHOD,
        EXPIRY_SKEW_SECONDS,
        MAX_RESPONSE_BYTES,
        READ_TIMEOUT_SECONDS,
    )

    assert EXPIRY_SKEW_SECONDS == 60
    assert DEFAULT_EXPIRES_IN_SECONDS == 300
    assert MAX_RESPONSE_BYTES == 64 * 1024
    assert CONNECT_TIMEOUT_SECONDS == 10
    assert READ_TIMEOUT_SECONDS == 30
    assert DEFAULT_TOKEN_AUTH_METHOD == "client_secret_post"
    assert "client_secret_post" in ALLOWED_AUTH_METHODS
    assert "client_secret_basic" in ALLOWED_AUTH_METHODS
    assert isinstance(DEFAULT_STORAGE_KEY, str) and DEFAULT_STORAGE_KEY


def test_client_credentials_module_importable():
    # The submodule path itself should resolve, in case callers want
    # access to internal helpers (e.g. the SSRF validator) for tests.
    from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
        client_credentials,
    )

    assert hasattr(client_credentials, "OAuth2ClientCredentialsProvider")

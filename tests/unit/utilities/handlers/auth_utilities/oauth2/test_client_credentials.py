"""Tests for OAuth2ClientCredentialsProvider."""

from __future__ import annotations

import base64
import logging
import threading
import time
from typing import Optional

import pytest
import requests

from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import (
    OAuth2ClientCredentialsProvider,
)
from mindsdb.integrations.utilities.handlers.auth_utilities.oauth2 import client_credentials as cc_module


VALID_TOKEN_URL = "https://example.com/oauth/token"
CLIENT_ID = "client-id-abc"
CLIENT_SECRET = "client-secret-xyz"


def _conn(**overrides):
    """Build a connection_data dict with valid defaults."""
    data = {
        "token_url": VALID_TOKEN_URL,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    data.update(overrides)
    return data


class FakeResponse:
    """Minimal stand-in for requests.Response used by token-request tests."""

    def __init__(
        self,
        status_code: int = 200,
        json_body: Optional[dict] = None,
        raw_body: Optional[bytes] = None,
        is_redirect: bool = False,
    ) -> None:
        self.status_code = status_code
        if raw_body is not None:
            self._body = raw_body
        elif json_body is not None:
            import json as _json

            self._body = _json.dumps(json_body).encode("utf-8")
        else:
            self._body = b""
        self.is_redirect = is_redirect

    def iter_content(self, chunk_size: int = 4096):
        # Yield in chunks of chunk_size
        for i in range(0, len(self._body), chunk_size):
            yield self._body[i : i + chunk_size]

    def close(self):
        pass


class FakeStorage:
    """Minimal in-memory stand-in for handler_storage.encrypted_json_*."""

    def __init__(self) -> None:
        self._data: dict = {}
        self.set_calls = 0
        self.get_calls = 0

    def encrypted_json_get(self, key: str):
        self.get_calls += 1
        return self._data.get(key)

    def encrypted_json_set(self, key: str, value) -> None:
        self.set_calls += 1
        if value is None:
            self._data.pop(key, None)
        else:
            self._data[key] = value


class FailingStorage:
    """A handler_storage stand-in whose set always raises."""

    def __init__(self) -> None:
        self.set_calls = 0
        self.get_calls = 0

    def encrypted_json_get(self, key: str):
        self.get_calls += 1
        return None

    def encrypted_json_set(self, key: str, value) -> None:
        self.set_calls += 1
        raise RuntimeError("storage offline")


def _bypass_dns(monkeypatch):
    """Stub socket.getaddrinfo so the public-host SSRF check does not hit DNS.

    Returns a public IP (1.2.3.4) which passes all forbidden-range checks.
    """

    def fake_getaddrinfo(host, *args, **kwargs):
        return [(2, 1, 6, "", ("1.2.3.4", 0))]

    monkeypatch.setattr(cc_module.socket, "getaddrinfo", fake_getaddrinfo)


class TestConstructionValidation:
    def test_unsupported_auth_method_raises(self, monkeypatch):
        _bypass_dns(monkeypatch)
        with pytest.raises(ValueError) as excinfo:
            OAuth2ClientCredentialsProvider(_conn(token_auth_method="client_secret_jwt"))
        msg = str(excinfo.value)
        assert "client_secret_post" in msg
        assert "client_secret_basic" in msg

    def test_client_secret_post_accepted(self, monkeypatch):
        _bypass_dns(monkeypatch)
        OAuth2ClientCredentialsProvider(_conn(token_auth_method="client_secret_post"))

    def test_client_secret_basic_accepted(self, monkeypatch):
        _bypass_dns(monkeypatch)
        OAuth2ClientCredentialsProvider(_conn(token_auth_method="client_secret_basic"))

    def test_default_token_auth_method_is_client_secret_post(self, monkeypatch):
        _bypass_dns(monkeypatch)
        provider = OAuth2ClientCredentialsProvider(_conn())
        assert provider.token_auth_method == "client_secret_post"

    def test_missing_token_url_raises(self):
        with pytest.raises(ValueError) as excinfo:
            OAuth2ClientCredentialsProvider({"client_id": CLIENT_ID, "client_secret": CLIENT_SECRET})
        assert "token_url" in str(excinfo.value)

    def test_missing_client_id_raises(self, monkeypatch):
        _bypass_dns(monkeypatch)
        with pytest.raises(ValueError) as excinfo:
            OAuth2ClientCredentialsProvider({"token_url": VALID_TOKEN_URL, "client_secret": CLIENT_SECRET})
        assert "client_id" in str(excinfo.value)

    def test_missing_client_secret_raises(self, monkeypatch):
        _bypass_dns(monkeypatch)
        with pytest.raises(ValueError) as excinfo:
            OAuth2ClientCredentialsProvider({"token_url": VALID_TOKEN_URL, "client_id": CLIENT_ID})
        assert "client_secret" in str(excinfo.value)

    def test_non_dict_connection_data_raises(self):
        with pytest.raises(TypeError):
            OAuth2ClientCredentialsProvider("not-a-dict")

    @pytest.mark.parametrize(
        "url",
        [
            "http://localhost/oauth/token",
            "http://127.0.0.1/oauth/token",
            "http://10.0.0.1/oauth/token",
            "http://169.254.169.254/oauth/token",
            "http://192.168.1.1/oauth/token",
            "http://[::1]/oauth/token",
            "file:///etc/passwd",
            "ftp://example.com/oauth",
        ],
    )
    def test_ssrf_rules_reject_url(self, url):
        with pytest.raises(ValueError):
            OAuth2ClientCredentialsProvider(_conn(token_url=url))

    def test_http_url_accepted_with_warning(self, monkeypatch, caplog):
        _bypass_dns(monkeypatch)
        with caplog.at_level(logging.WARNING, logger=cc_module.logger.name):
            OAuth2ClientCredentialsProvider(_conn(token_url="http://example.com/oauth"))
        assert any("http://" in r.message or "unencrypted" in r.message for r in caplog.records)

    def test_https_url_accepted_no_warning(self, monkeypatch, caplog):
        _bypass_dns(monkeypatch)
        with caplog.at_level(logging.WARNING, logger=cc_module.logger.name):
            OAuth2ClientCredentialsProvider(_conn(token_url="https://example.com/oauth"))
        assert not any(r.levelno == logging.WARNING for r in caplog.records)


class TestRequestShape:
    def _provider(self, monkeypatch, **overrides):
        _bypass_dns(monkeypatch)
        return OAuth2ClientCredentialsProvider(_conn(**overrides))

    def test_client_secret_post_places_credentials_in_body(self, monkeypatch):
        provider = self._provider(monkeypatch, token_auth_method="client_secret_post")
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["url"] = url
            captured["data"] = data
            captured["headers"] = headers
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()

        assert captured["data"]["client_id"] == CLIENT_ID
        assert captured["data"]["client_secret"] == CLIENT_SECRET
        assert "Authorization" not in captured["headers"]

    def test_client_secret_basic_uses_auth_header(self, monkeypatch):
        provider = self._provider(monkeypatch, token_auth_method="client_secret_basic")
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            captured["headers"] = headers
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()

        expected = "Basic " + base64.b64encode(f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")).decode("ascii")
        assert captured["headers"]["Authorization"] == expected
        assert "client_id" not in captured["data"]
        assert "client_secret" not in captured["data"]

    def test_grant_type_always_present(self, monkeypatch):
        provider = self._provider(monkeypatch)
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert captured["data"]["grant_type"] == "client_credentials"

    def test_scope_string_included(self, monkeypatch):
        provider = self._provider(monkeypatch, scope="read:foo write:bar")
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert captured["data"]["scope"] == "read:foo write:bar"

    def test_scope_list_joined_with_space(self, monkeypatch):
        provider = self._provider(monkeypatch, scope=["read:foo", "write:bar"])
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert captured["data"]["scope"] == "read:foo write:bar"

    def test_audience_included_when_configured(self, monkeypatch):
        provider = self._provider(monkeypatch, audience="https://api.example.com")
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert captured["data"]["audience"] == "https://api.example.com"

    def test_audience_omitted_when_none(self, monkeypatch):
        provider = self._provider(monkeypatch)
        captured = {}

        def fake_post(url, data=None, headers=None, **_):
            captured["data"] = data
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert "audience" not in captured["data"]

    def test_redirects_disabled_and_timeouts_set(self, monkeypatch):
        provider = self._provider(monkeypatch)
        captured = {}

        def fake_post(url, data=None, headers=None, timeout=None, allow_redirects=None, **_):
            captured["timeout"] = timeout
            captured["allow_redirects"] = allow_redirects
            return FakeResponse(json_body={"access_token": "T", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        assert captured["allow_redirects"] is False
        assert captured["timeout"] == (10, 30)


# ---------------------------------------------------------------------------
# Response handling
# ---------------------------------------------------------------------------


class TestResponseHandling:
    def _provider(self, monkeypatch, **overrides):
        _bypass_dns(monkeypatch)
        return OAuth2ClientCredentialsProvider(_conn(**overrides))

    def test_success_caches_with_correct_expires_at(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "AT", "expires_in": 3600}),
        )
        before = time.time()
        token = provider.get_access_token()
        after = time.time()
        assert token == "AT"
        cached = provider._read_cache()
        # expires_at = now + 3600 - 60 (skew)
        assert before + 3600 - 60 - 1 <= cached["expires_at"] <= after + 3600 - 60 + 1

    def test_missing_access_token_fails(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"expires_in": 3600}),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        # Safe message: no client_secret leak
        assert CLIENT_SECRET not in str(excinfo.value)

    @pytest.mark.parametrize("token_type", ["Bearer", "bearer", "BEARER"])
    def test_token_type_case_insensitive(self, monkeypatch, token_type):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(
                json_body={"access_token": "AT", "token_type": token_type, "expires_in": 3600}
            ),
        )
        assert provider.get_access_token() == "AT"

    def test_missing_token_type_defaults_to_bearer(self, monkeypatch):
        # No token_type field at all in the response — provider must accept it
        # and treat it as Bearer per RFC 6749 §5.1.
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "AT", "expires_in": 3600}),
        )
        assert provider.get_access_token() == "AT"

    @pytest.mark.parametrize("token_type", ["MAC", "DPoP", "Token"])
    def test_unsupported_token_type_fails(self, monkeypatch, token_type):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(
                json_body={"access_token": "AT", "token_type": token_type, "expires_in": 3600}
            ),
        )
        with pytest.raises(RuntimeError):
            provider.get_access_token()

    @pytest.mark.parametrize("expires_in", [None, 0, -100, "abc"])
    def test_invalid_expires_in_defaults_to_300(self, monkeypatch, caplog, expires_in):
        provider = self._provider(monkeypatch)
        body = {"access_token": "AT"}
        if expires_in is not None:
            body["expires_in"] = expires_in
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body=body),
        )
        before = time.time()
        with caplog.at_level(logging.WARNING, logger=cc_module.logger.name):
            provider.get_access_token()
        after = time.time()
        cached = provider._read_cache()
        # expires_at = now + 300 - 60 = now + 240
        assert before + 240 - 1 <= cached["expires_at"] <= after + 240 + 1
        if expires_in is None:
            # WARNING log emitted at least once when explicitly missing
            warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "expires_in" in r.message]
            assert warnings, "expected a WARNING log mentioning expires_in"


class TestCaching:
    def _provider(self, monkeypatch, **overrides):
        _bypass_dns(monkeypatch)
        return OAuth2ClientCredentialsProvider(_conn(**overrides))

    def test_cached_token_reused(self, monkeypatch):
        provider = self._provider(monkeypatch)
        calls = {"n": 0}

        def fake_post(*a, **kw):
            calls["n"] += 1
            return FakeResponse(json_body={"access_token": "AT", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        provider.get_access_token()
        provider.get_access_token()
        provider.get_access_token()
        assert calls["n"] == 1

    def test_token_within_skew_triggers_refresh(self, monkeypatch):
        provider = self._provider(monkeypatch)
        # Manually inject a token that is "expired" (skew already elapsed)
        provider._memory_cache = {
            "access_token": "OLD",
            "token_type": "Bearer",
            "expires_at": time.time() - 1,
        }
        calls = {"n": 0}

        def fake_post(*a, **kw):
            calls["n"] += 1
            return FakeResponse(json_body={"access_token": "NEW", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        assert provider.get_access_token() == "NEW"
        assert calls["n"] == 1

    def test_expired_token_triggers_refresh(self, monkeypatch):
        provider = self._provider(monkeypatch)
        provider._memory_cache = {
            "access_token": "OLD",
            "token_type": "Bearer",
            "expires_at": time.time() - 3600,
        }
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "NEW", "expires_in": 3600}),
        )
        assert provider.get_access_token() == "NEW"

    def test_clear_cached_token_clears_cache_and_refetches(self, monkeypatch):
        provider = self._provider(monkeypatch)
        calls = {"n": 0}
        tokens = iter(["FIRST", "SECOND"])

        def fake_post(*a, **kw):
            calls["n"] += 1
            return FakeResponse(json_body={"access_token": next(tokens), "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        assert provider.get_access_token() == "FIRST"
        provider.clear_cached_token()
        assert provider.get_access_token() == "SECOND"
        assert calls["n"] == 2


class TestConcurrency:
    def test_concurrent_get_token_makes_single_http_call(self, monkeypatch):
        """Two threads call get_access_token() with empty cache simultaneously.

        Forces the double-checked-lock scenario: both threads must complete
        their FIRST cache read (and observe empty) before either acquires the
        lock. With the second read inside the lock, the second thread observes
        the populated cache and skips the HTTP call. Without it, both threads
        would issue the HTTP request — exactly the bug the lock prevents.
        """
        _bypass_dns(monkeypatch)
        provider = OAuth2ClientCredentialsProvider(_conn())

        call_count = {"n": 0}
        sync_counter = {"value": 0}
        sync_lock = threading.Lock()
        first_read_barrier = threading.Barrier(2)

        original_read_cache = provider._read_cache

        def synced_read_cache():
            result = original_read_cache()
            with sync_lock:
                sync_counter["value"] += 1
                n = sync_counter["value"]
            # Only the first read from each thread (the pre-lock read) blocks
            # on the barrier. Inside-lock reads (calls 3 and 4) pass through.
            if n <= 2:
                first_read_barrier.wait(timeout=5)
            return result

        monkeypatch.setattr(provider, "_read_cache", synced_read_cache)

        def fake_post(*a, **kw):
            call_count["n"] += 1
            # Hold the lock briefly so the second thread is queued behind it.
            time.sleep(0.05)
            return FakeResponse(json_body={"access_token": "SHARED", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)

        results = {}

        def worker(idx):
            results[idx] = provider.get_access_token()

        t1 = threading.Thread(target=worker, args=(1,))
        t2 = threading.Thread(target=worker, args=(2,))
        t1.start()
        t2.start()
        t1.join(timeout=5)
        t2.join(timeout=5)

        assert results[1] == "SHARED"
        assert results[2] == "SHARED"
        assert call_count["n"] == 1, (
            "Double-checked locking failed: both threads issued an HTTP call. "
            "The second read inside the lock is missing or broken."
        )


class TestStorage:
    def test_token_persists_across_provider_instances(self, monkeypatch):
        _bypass_dns(monkeypatch)
        storage = FakeStorage()
        calls = {"n": 0}

        def fake_post(*a, **kw):
            calls["n"] += 1
            return FakeResponse(json_body={"access_token": "PERSIST", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)

        p1 = OAuth2ClientCredentialsProvider(
            _conn(),
            handler_storage=storage,
            storage_key="oauth_test",
        )
        assert p1.get_access_token() == "PERSIST"

        p2 = OAuth2ClientCredentialsProvider(
            _conn(),
            handler_storage=storage,
            storage_key="oauth_test",
        )
        assert p2.get_access_token() == "PERSIST"
        assert calls["n"] == 1

    def test_in_memory_cache_when_storage_none(self, monkeypatch):
        _bypass_dns(monkeypatch)
        provider = OAuth2ClientCredentialsProvider(_conn())
        calls = {"n": 0}

        def fake_post(*a, **kw):
            calls["n"] += 1
            return FakeResponse(json_body={"access_token": "MEM", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        assert provider.get_access_token() == "MEM"
        assert provider.get_access_token() == "MEM"
        assert calls["n"] == 1

    def test_storage_set_failure_falls_back_to_memory(self, monkeypatch, caplog):
        _bypass_dns(monkeypatch)
        storage = FailingStorage()
        provider = OAuth2ClientCredentialsProvider(
            _conn(),
            handler_storage=storage,
        )

        def fake_post(*a, **kw):
            return FakeResponse(json_body={"access_token": "FB", "expires_in": 3600})

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        with caplog.at_level(logging.DEBUG, logger=cc_module.logger.name):
            assert provider.get_access_token() == "FB"
        # Subsequent get_access_token uses in-memory fallback
        assert provider.get_access_token() == "FB"
        assert provider._memory_cache is not None
        assert any("in-memory" in r.message or "fallback" in r.message.lower() for r in caplog.records)

    def test_cache_does_not_contain_credentials(self, monkeypatch):
        _bypass_dns(monkeypatch)
        storage = FakeStorage()
        provider = OAuth2ClientCredentialsProvider(
            _conn(scope="read:foo", audience="https://api.example.com"),
            handler_storage=storage,
            storage_key="oauth_test",
        )
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "AT", "expires_in": 3600}),
        )
        provider.get_access_token()
        cached = storage._data["oauth_test"]
        as_text = repr(cached)
        assert CLIENT_SECRET not in as_text
        assert CLIENT_ID not in as_text
        assert VALID_TOKEN_URL not in as_text
        assert "read:foo" not in as_text
        assert "https://api.example.com" not in as_text


class TestCurrentSecrets:
    def _provider(self, monkeypatch):
        _bypass_dns(monkeypatch)
        return OAuth2ClientCredentialsProvider(_conn())

    def test_no_cached_token_returns_empty(self, monkeypatch):
        provider = self._provider(monkeypatch)
        assert provider.current_secrets() == []

    def test_cached_valid_token_returned(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "S3CR3T", "expires_in": 3600}),
        )
        provider.get_access_token()
        assert provider.current_secrets() == ["S3CR3T"]

    def test_after_clear_cached_token_returns_empty(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "S3CR3T", "expires_in": 3600}),
        )
        provider.get_access_token()
        provider.clear_cached_token()
        assert provider.current_secrets() == []

    def test_current_secrets_never_returns_client_secret(self, monkeypatch):
        # Across uncached, cached, and post-clear states, the client_secret
        # must never appear in current_secrets() — that list feeds the
        # response-scrub layer and exposing the static credential there
        # would risk redacting upstream payloads that legitimately contain
        # the same string, but more importantly it would imply the secret
        # is in some live cache, which it must not be.
        provider = self._provider(monkeypatch)

        # Uncached
        assert CLIENT_SECRET not in provider.current_secrets()

        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "AT", "expires_in": 3600}),
        )
        provider.get_access_token()

        # Cached — only the access_token should show up
        secrets = provider.current_secrets()
        assert secrets == ["AT"]
        assert CLIENT_SECRET not in secrets
        assert CLIENT_ID not in secrets

        provider.clear_cached_token()
        assert CLIENT_SECRET not in provider.current_secrets()


class TestErrorSanitization:
    def _provider(self, monkeypatch):
        _bypass_dns(monkeypatch)
        return OAuth2ClientCredentialsProvider(_conn())

    def test_failure_message_does_not_leak_client_secret(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(status_code=500, json_body={"error": "boom"}),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        assert CLIENT_SECRET not in str(excinfo.value)

    def test_failure_message_does_not_leak_access_token(self, monkeypatch):
        provider = self._provider(monkeypatch)
        # First, succeed and cache
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(json_body={"access_token": "very-secret-token", "expires_in": 1}),
        )
        provider.get_access_token()
        # Now force expiry and make next call fail
        provider._memory_cache["expires_at"] = time.time() - 1
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(status_code=401, json_body={"error": "invalid_grant"}),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        assert "very-secret-token" not in str(excinfo.value)

    def test_401_includes_provider_error_fields(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(
                status_code=401,
                json_body={
                    "error": "invalid_client",
                    "error_description": "Client authentication failed",
                },
            ),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        msg = str(excinfo.value)
        assert "invalid_client" in msg
        assert "Client authentication failed" in msg
        assert CLIENT_SECRET not in msg
        assert CLIENT_ID in msg or "client_id" in msg

    def test_redirect_response_treated_as_error(self, monkeypatch):
        provider = self._provider(monkeypatch)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(status_code=302, is_redirect=True),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        assert "redirect" in str(excinfo.value).lower()

    def test_response_size_cap_aborts(self, monkeypatch):
        provider = self._provider(monkeypatch)
        oversize = b"x" * (cc_module.MAX_RESPONSE_BYTES + 100)
        monkeypatch.setattr(
            cc_module.requests,
            "post",
            lambda *a, **kw: FakeResponse(status_code=200, raw_body=oversize),
        )
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        assert "exceeded" in str(excinfo.value).lower()

    def test_transport_error_message_redacts_secret(self, monkeypatch):
        provider = self._provider(monkeypatch)

        def fake_post(*a, **kw):
            raise requests.ConnectionError(f"oops while sending client_secret={CLIENT_SECRET}")

        monkeypatch.setattr(cc_module.requests, "post", fake_post)
        with pytest.raises(RuntimeError) as excinfo:
            provider.get_access_token()
        chained = excinfo.value.__cause__
        assert CLIENT_SECRET not in str(excinfo.value)
        assert CLIENT_SECRET not in str(chained)

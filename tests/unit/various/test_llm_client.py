"""
Unit tests for LLMClient — focusing on the Ollama provider path.

Regression tests for:
  https://github.com/mindsdb/mindsdb/issues/12339

When default_llm is configured with Ollama and the config uses 'model'
as the key (instead of 'model_name'), LLMClient now normalises it
correctly instead of raising a misleading api_key error.
"""

import unittest
import sys
import types
from unittest.mock import MagicMock, patch

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient


# Lightweight stub so LLMClient can be imported without the full Mindsdb stack.
_HANDLER_UTILS_KEY = "mindsdb.integrations.utilities.handler_utils"
_stub = types.ModuleType(_HANDLER_UTILS_KEY)
_stub.get_api_key = MagicMock(return_value=None)
sys.modules.setdefault(_HANDLER_UTILS_KEY, _stub)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_params_with_model_key():
    """Params where the model is specified under the 'model' key."""
    return {
        "provider": "ollama",
        "model": "llama3.2",
        "base_url": "http://host.docker.internal:11434",
        "api_keys": {},
    }


def _make_params_legacy():
    """
    Params built without going through get_llm_config (e.g. env-var path or
    older code) — use 'model_name'.  The fix must not break this path.
    """
    return {
        "provider": "ollama",
        "model_name": "llama3.2",  # <-- legacy key
        "base_url": "http://host.docker.internal:11434",
        "api_keys": {},
    }


def _build_client(params):
    """
    Instantiate LLMClient with a patched OpenAI constructor so no real network
    call is made.  Returns (client, kwargs_passed_to_OpenAI).
    """
    captured = {}

    class _FakeOpenAI:
        def __init__(self_, **kwargs):
            captured.update(kwargs)

    with patch("mindsdb.interfaces.knowledge_base.llm_client.OpenAI", side_effect=_FakeOpenAI):
        client = LLMClient(params)

    return client, captured


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestLLMClientOllama(unittest.TestCase):
    """Regression tests for issue #12339 — Ollama as default_llm."""

    # ------------------------------------------------------------------
    # Core regression: params arriving via OllamaConfig.model_dump()
    # ------------------------------------------------------------------

    def test_model_key_does_not_raise(self):
        """LLMClient must not raise when params carry 'model' instead of 'model_name'."""
        params = _make_params_with_model_key()
        try:
            _build_client(params)
        except Exception as exc:
            self.fail(f"LLMClient raised unexpectedly with 'model' key: {exc}")

    def test_model_name_normalised_from_model_key(self):
        """
        After construction, self.params['model_name'] must equal the value
        that arrived under the 'model' key so that completion() and
        embeddings() can reference it uniformly.
        """
        params = _make_params_with_model_key()
        client, _ = _build_client(params)
        self.assertEqual(client.model_name, "llama3.2")

    def test_placeholder_api_key_injected_when_absent(self):
        """
        Ollama needs no real API key.  A non-empty placeholder must be
        injected so the OpenAI-compatible client doesn't raise on construction.
        """
        params = _make_params_with_model_key()
        _, openai_kwargs = _build_client(params)
        self.assertIn("api_key", openai_kwargs)
        self.assertTrue(openai_kwargs["api_key"])

    def test_base_url_forwarded_to_openai(self):
        """base_url must reach the OpenAI-compatible client unchanged."""
        params = _make_params_with_model_key()
        _, openai_kwargs = _build_client(params)
        self.assertEqual(openai_kwargs["base_url"], "http://host.docker.internal:11434")

    def test_provider_not_forwarded_to_openai(self):
        """'provider' must be stripped before the OpenAI() call."""
        params = _make_params_with_model_key()
        _, openai_kwargs = _build_client(params)
        self.assertNotIn("provider", openai_kwargs)

    # ------------------------------------------------------------------
    # Backward compatibility: legacy params using 'model_name'
    # ------------------------------------------------------------------

    def test_legacy_model_name_key_still_works(self):
        """
        Params built with 'model_name' directly (env-var path, older code)
        must continue to work after the fix.
        """
        params = _make_params_legacy()
        try:
            client, _ = _build_client(params)
        except Exception as exc:
            self.fail(f"LLMClient raised with legacy 'model_name' key: {exc}")
        self.assertEqual(client.model_name, "llama3.2")

    def test_explicit_api_key_preserved(self):
        """
        If a real api_key is provided it must be forwarded as-is, not
        overwritten by the placeholder.
        """
        params = {**_make_params_with_model_key(), "api_key": "real-secret"}
        _, openai_kwargs = _build_client(params)
        self.assertEqual(openai_kwargs["api_key"], "real-secret")

    def test_missing_model_name_raises_clear_error(self):
        """
        If neither 'model' nor 'model_name' is present, LLMClient must raise
        a clear ValueError rather than a cryptic api_key error.
        """
        params = {"provider": "ollama", "base_url": "http://host.docker.internal:11434", "api_keys": {}}
        with self.assertRaises(ValueError):
            _build_client(params)


if __name__ == "__main__":
    unittest.main()

"""
Unit tests for LLMClient — focusing on the Ollama provider path.

Regression tests for:
  https://github.com/mindsdb/mindsdb/issues/12339

  Bug: when default_llm is set to Ollama in config.json (or the MindsDB UI),
  the LLM() SQL function raises:
    "The api_key client option must be set ... OPENAI_API_KEY"

  Root cause: get_llm_config() returns an OllamaConfig whose model field is
  named 'model', but LLMClient.__init__ tried to pop 'model_name' — causing a
  KeyError that left kwargs in a broken state, which then crashed the OpenAI()
  constructor with a missing-api-key error.

  Fix: LLMClient now accepts either 'model' or 'model_name', strips OllamaConfig-
  specific keys that the OpenAI-compatible constructor does not understand, and
  injects a placeholder api_key so construction succeeds.

The stub for mindsdb.integrations.utilities.handler_utils is injected by
conftest.py in this directory before collection, so it is scoped to this
test package and does not affect other test modules.
"""

import unittest
from unittest.mock import patch

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_params_via_model_dump():
    """
    Simulate the dict that controller.py produces after:

        cfg = get_llm_config('ollama', chat_model_params)
        params = {k: v for k, v in cfg.model_dump(by_alias=True).items() if v is not None}
        params['provider'] = 'ollama'

    OllamaConfig declares its model field as 'model' (not 'model_name'), so
    after model_dump the dict uses 'model' as the key.  This was the bug.
    """
    return {
        "provider": "ollama",
        "model": "llama3.2",  # <-- key produced by OllamaConfig.model_dump()
        "base_url": "http://host.docker.internal:11434",
        "temperature": 0.0,
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

    def test_model_dump_key_does_not_raise(self):
        """
        LLMClient must not raise when params carry 'model' instead of
        'model_name' (the output of OllamaConfig.model_dump()).

        Before the fix, kwargs.pop('model_name') raised KeyError, which
        surfaced as "The api_key client option must be set … OPENAI_API_KEY".
        """
        params = _make_params_via_model_dump()
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
        params = _make_params_via_model_dump()
        client, _ = _build_client(params)
        self.assertEqual(client.params["model_name"], "llama3.2")

    def test_placeholder_api_key_injected_when_absent(self):
        """
        Ollama needs no real API key.  A non-empty placeholder must be
        injected so the OpenAI-compatible client doesn't raise on construction.
        """
        params = _make_params_via_model_dump()
        _, openai_kwargs = _build_client(params)
        self.assertIn("api_key", openai_kwargs)
        self.assertTrue(openai_kwargs["api_key"])

    def test_base_url_forwarded_to_openai(self):
        """base_url must reach the OpenAI-compatible client unchanged."""
        params = _make_params_via_model_dump()
        _, openai_kwargs = _build_client(params)
        self.assertEqual(openai_kwargs["base_url"], "http://host.docker.internal:11434")

    def test_ollama_only_keys_not_forwarded_to_openai(self):
        """
        OllamaConfig-specific fields (temperature, top_p, top_k, …) must be
        stripped before calling OpenAI() — it does not accept them and would
        raise TypeError.
        """
        params = {
            **_make_params_via_model_dump(),
            "top_p": 0.9,
            "top_k": 40,
            "num_ctx": 4096,
            "repeat_penalty": 1.1,
            "template": "my-template",
            "headers": {"X-Custom": "val"},
        }
        _, openai_kwargs = _build_client(params)

        disallowed = {
            "temperature",
            "top_p",
            "top_k",
            "timeout",
            "format",
            "headers",
            "num_predict",
            "num_ctx",
            "num_gpu",
            "repeat_penalty",
            "stop",
            "template",
            "api_keys",
            "provider",
            "model",
            "model_name",
        }
        leaked = disallowed & set(openai_kwargs.keys())
        self.assertEqual(leaked, set(), msg=f"Keys leaked to OpenAI(): {leaked}")

    def test_provider_not_forwarded_to_openai(self):
        """'provider' must be stripped before the OpenAI() call."""
        params = _make_params_via_model_dump()
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
        self.assertEqual(client.params["model_name"], "llama3.2")

    def test_explicit_api_key_preserved(self):
        """
        If a real api_key is provided it must be forwarded as-is, not
        overwritten by the placeholder.
        """
        params = {**_make_params_via_model_dump(), "api_key": "real-secret"}
        _, openai_kwargs = _build_client(params)
        self.assertEqual(openai_kwargs["api_key"], "real-secret")


if __name__ == "__main__":
    unittest.main()

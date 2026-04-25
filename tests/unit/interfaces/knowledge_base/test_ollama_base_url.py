"""Tests for ``LLMClient._normalize_ollama_base_url`` (gh-11910).

When a knowledge base is configured with an Ollama embedding model, the
OpenAI Python SDK appends route paths (``/embeddings``, ``/chat/completions``)
directly onto whatever ``base_url`` the client was constructed with. Ollama
serves its OpenAI-compatible API only under the ``/v1`` prefix, so a value
like ``http://localhost:11434`` resolves to ``http://localhost:11434/embeddings``
which Ollama does not expose and answers with ``404 Not Found``.

These tests pin the normalization rules so future refactors cannot silently
re-introduce the original bug.
"""

import pytest

from mindsdb.interfaces.knowledge_base.llm_client import LLMClient


OLLAMA_DEFAULT = "http://localhost:11434/v1"


class TestNormalizeOllamaBaseUrl:
    @pytest.mark.parametrize("missing", [None, ""])
    def test_missing_base_url_falls_back_to_local_default(self, missing):
        assert LLMClient._normalize_ollama_base_url(missing) == OLLAMA_DEFAULT

    def test_user_url_without_v1_gets_v1_appended(self):
        # The exact case reported in gh-11910 — first user attempt.
        assert LLMClient._normalize_ollama_base_url("http://localhost:11434") == "http://localhost:11434/v1"

    def test_trailing_slash_is_collapsed_before_appending(self):
        assert LLMClient._normalize_ollama_base_url("http://localhost:11434/") == "http://localhost:11434/v1"

    def test_explicit_v1_is_left_unchanged(self):
        assert LLMClient._normalize_ollama_base_url("http://localhost:11434/v1") == "http://localhost:11434/v1"

    def test_explicit_v1_with_trailing_slash_is_normalized(self):
        # rstrip("/") collapses trailing slashes; presence of /v1 still detected.
        assert LLMClient._normalize_ollama_base_url("http://localhost:11434/v1/") == "http://localhost:11434/v1"

    def test_remote_host_without_v1_gets_v1_appended(self):
        assert (
            LLMClient._normalize_ollama_base_url("https://ollama.example.com:8080")
            == "https://ollama.example.com:8080/v1"
        )

    def test_url_with_v1_in_subpath_is_preserved(self):
        # Some users front Ollama with a reverse proxy under a sub-path.
        assert (
            LLMClient._normalize_ollama_base_url("http://gateway.local/ollama/v1") == "http://gateway.local/ollama/v1"
        )

    def test_v10_is_not_mistaken_for_v1(self):
        # Path-segment match must be exact: "v10" must not satisfy the "v1" check,
        # otherwise we would silently drop the append and break custom deployments.
        assert LLMClient._normalize_ollama_base_url("http://host/v10") == "http://host/v10/v1"

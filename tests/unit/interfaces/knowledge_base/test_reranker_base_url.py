"""Tests for BaseLLMReranker provider-specific default base_url (gh-11952).

When the Ollama provider is configured without an explicit base_url, the
fallback must be http://localhost:11434/v1 — not the OpenAI endpoint.
Before the fix, all Ollama reranker calls silently hit api.openai.com/v1
and failed with 404.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mindsdb.integrations.utilities.rag.settings import DEFAULT_LLM_ENDPOINT
from mindsdb.integrations.utilities.rag.rerankers.base_reranker import BaseLLMReranker

OLLAMA_DEFAULT_BASE_URL = "http://localhost:11434/v1"


def _make_reranker(**kwargs):
    """Construct a BaseLLMReranker with _init_client bypassed."""
    with patch.object(BaseLLMReranker, "_init_client", return_value=None):
        obj = BaseLLMReranker(**kwargs)
    return obj


class TestOllamaDefaultBaseUrl:
    """The client must be created with the Ollama-specific URL when base_url is omitted."""

    def test_ollama_without_base_url_uses_ollama_default(self):
        reranker = _make_reranker(provider="ollama", model="llama3.2", api_key="n/a")
        reranker.client = None  # force re-init

        with patch(
            "mindsdb.integrations.utilities.rag.rerankers.base_reranker.AsyncOpenAI"
        ) as mock_openai:
            reranker._init_client()

        _, kwargs = mock_openai.call_args
        assert kwargs.get("base_url") == OLLAMA_DEFAULT_BASE_URL, (
            f"Expected Ollama default {OLLAMA_DEFAULT_BASE_URL!r}, got {kwargs.get('base_url')!r}. "
            "Without this fix, Ollama reranker requests hit the OpenAI endpoint and fail with 404."
        )

    def test_ollama_with_explicit_base_url_respects_it(self):
        custom_url = "http://my-ollama-server:11434/v1"
        reranker = _make_reranker(provider="ollama", model="llama3.2", api_key="n/a", base_url=custom_url)
        reranker.client = None

        with patch(
            "mindsdb.integrations.utilities.rag.rerankers.base_reranker.AsyncOpenAI"
        ) as mock_openai:
            reranker._init_client()

        _, kwargs = mock_openai.call_args
        assert kwargs.get("base_url") == custom_url

    def test_openai_without_base_url_keeps_openai_default(self):
        reranker = _make_reranker(provider="openai", model="gpt-4o", api_key="sk-test")
        reranker.client = None

        with patch(
            "mindsdb.integrations.utilities.rag.rerankers.base_reranker.AsyncOpenAI"
        ) as mock_openai:
            reranker._init_client()

        _, kwargs = mock_openai.call_args
        assert kwargs.get("base_url") == DEFAULT_LLM_ENDPOINT, (
            "OpenAI provider without explicit base_url must still default to DEFAULT_LLM_ENDPOINT."
        )

    def test_openai_with_explicit_base_url_respects_it(self):
        custom_url = "https://my-openai-proxy.example.com/v1"
        reranker = _make_reranker(provider="openai", model="gpt-4o", api_key="sk-test", base_url=custom_url)
        reranker.client = None

        with patch(
            "mindsdb.integrations.utilities.rag.rerankers.base_reranker.AsyncOpenAI"
        ) as mock_openai:
            reranker._init_client()

        _, kwargs = mock_openai.call_args
        assert kwargs.get("base_url") == custom_url

    def test_ollama_default_is_not_openai_endpoint(self):
        """Regression: Ollama default must not be the OpenAI endpoint."""
        reranker = _make_reranker(provider="ollama", model="llama3.2", api_key="n/a")
        reranker.client = None

        with patch(
            "mindsdb.integrations.utilities.rag.rerankers.base_reranker.AsyncOpenAI"
        ) as mock_openai:
            reranker._init_client()

        _, kwargs = mock_openai.call_args
        assert kwargs.get("base_url") != DEFAULT_LLM_ENDPOINT, (
            "Ollama default base_url must not be the OpenAI API endpoint."
        )

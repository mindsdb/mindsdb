"""Utilities for working with agent providers.

These helpers are intentionally free of heavy optional dependencies so they can
be imported in lightweight builds where LangChain is not installed.
"""

from typing import Dict

from mindsdb.interfaces.agents.constants import (
    ANTHROPIC_CHAT_MODELS,
    GOOGLE_GEMINI_CHAT_MODELS,
    NVIDIA_NIM_CHAT_MODELS,
    OLLAMA_CHAT_MODELS,
    OPEN_AI_CHAT_MODELS,
    WRITER_CHAT_MODELS,
)


def get_llm_provider(args: Dict) -> str:
    """Infer the LLM provider from the supplied arguments."""

    # Prefer an explicitly provided provider.
    if "provider" in args:
        return args["provider"]

    model_name = args.get("model_name")
    if model_name in ANTHROPIC_CHAT_MODELS:
        return "anthropic"
    if model_name in OPEN_AI_CHAT_MODELS:
        return "openai"
    if model_name in OLLAMA_CHAT_MODELS:
        return "ollama"
    if model_name in NVIDIA_NIM_CHAT_MODELS:
        return "nvidia_nim"
    if model_name in GOOGLE_GEMINI_CHAT_MODELS:
        return "google"
    if model_name in WRITER_CHAT_MODELS:
        return "writer"

    raise ValueError("Invalid model name. Please define a supported llm provider")

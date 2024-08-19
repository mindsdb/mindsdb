"""This module provides convenient tracing wrappers for popular libraries."""

from langsmith.wrappers._openai import wrap_openai

__all__ = ["wrap_openai"]

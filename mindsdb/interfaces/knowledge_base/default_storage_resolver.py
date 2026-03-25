import os
from typing import Any

from mindsdb.utilities.config import config


def _normalize_engine_name(engine: str | None) -> str | None:
    if engine is None:
        return None
    normalized = engine.strip().lower()
    if normalized in ("duckdb_faiss", "faiss"):
        return "faiss"
    if normalized == "pgvector":
        return "pgvector"
    return normalized or None


def _get_env_available_engines() -> list[str]:
    env_value = os.environ.get("KNOWLEDGE_BASES_STORAGE", "")
    engines: list[str] = []

    if env_value != "":
        for item in env_value.split(","):
            engine = _normalize_engine_name(item)
            if engine and engine not in engines:
                engines.append(engine)
        return engines

    # Default available engines when explicit env override is not provided.
    engines.append("faiss")
    if os.environ.get("KB_PGVECTOR_URL"):
        engines.append("pgvector")
    return engines


def get_env_available_engines() -> list[str]:
    return _get_env_available_engines()


def get_knowledge_base_storage_config(config_obj=None) -> str | None:
    config_obj = config_obj or config
    storage = config_obj.get("knowledge_bases", {}).get("storage", None)

    if storage is None:
        return None

    if isinstance(storage, list):
        if len(storage) == 0:
            return None
        storage = storage[0]

    if not isinstance(storage, str):
        raise ValueError("knowledge_bases.storage must be a string value")

    return _normalize_engine_name(storage)


def _unique_default_first(default: str | None, ordered: list[str]) -> list[str]:
    """Return `ordered` with `default` first if set, dropping later duplicates."""
    out: list[str] = []
    seen: set[str] = set()
    for engine in ([default] if default else []) + ordered:
        if engine not in seen:
            seen.add(engine)
            out.append(engine)
    return out


def resolve_default_storage_engines(config_obj=None) -> dict[str, Any]:
    configured = get_knowledge_base_storage_config(config_obj)
    pgvector_enabled = os.environ.get("KB_PGVECTOR_URL") is not None
    available = _get_env_available_engines()

    if configured and configured not in available:
        available = [configured, *available]

    default = configured
    if default is None:
        default = "pgvector" if pgvector_enabled else None
    if default is None and available:
        default = available[0]

    candidates = _unique_default_first(default, available)
    available_set = set(available)
    resolved_storage = [
        {
            "engine": name,
            "available": name in available_set,
            "default": name == default,
            "source": "config" if configured == name else "fallback",
        }
        for name in candidates
    ]

    return {
        "storage": configured,
        "resolved_storage": resolved_storage,
        "default_storage": default,
        "available_vector_engines": available,
        "pgvector_enabled": pgvector_enabled,
    }

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
    if env_value == "":
        return []
    engines = []
    for item in env_value.split(","):
        engine = _normalize_engine_name(item)
        if engine and engine not in engines:
            engines.append(engine)
    return engines


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


def resolve_default_storage_engines(integration_controller, config_obj=None) -> dict[str, Any]:
    configured_storage = get_knowledge_base_storage_config(config_obj)
    pgvector_enabled = os.environ.get("KB_PGVECTOR_URL") is not None
    available_vector_engines = _get_env_available_engines()
    if not available_vector_engines and pgvector_enabled:
        available_vector_engines = ["pgvector"]

    if configured_storage and configured_storage not in available_vector_engines:
        available_vector_engines = [configured_storage] + available_vector_engines

    default_engine = configured_storage or (available_vector_engines[0] if available_vector_engines else None)

    candidate_storage = []
    if default_engine:
        candidate_storage.append(default_engine)
    for engine in available_vector_engines:
        if engine not in candidate_storage:
            candidate_storage.append(engine)

    resolved_storage = []

    for engine in candidate_storage:
        resolved_storage.append(
            {
                "engine": engine,
                "available": engine in available_vector_engines,
                "default": engine == default_engine,
                "source": "config" if configured_storage == engine else "fallback",
            }
        )

    return {
        "storage": configured_storage,
        "resolved_storage": resolved_storage,
        "default_storage": default_engine,
        "available_vector_engines": available_vector_engines,
        "pgvector_enabled": pgvector_enabled,
    }

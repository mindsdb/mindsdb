import os
from typing import Any

from mindsdb.utilities.config import config


def get_knowledge_base_storage_config(config_obj=None) -> list[str]:
    config_obj = config_obj or config
    storage = config_obj.get("knowledge_bases", {}).get("storage", [])

    if storage is None:
        return []
    if isinstance(storage, str):
        storage = [storage]
    if not isinstance(storage, list):
        raise ValueError("knowledge_bases.storage must be a list of storage engine names")

    normalized = []
    seen = set()
    for item in storage:
        if not isinstance(item, str):
            raise ValueError("knowledge_bases.storage must contain only string values")
        engine = item.strip()
        if engine and engine not in seen:
            normalized.append(engine)
            seen.add(engine)

    return normalized


def resolve_default_storage_engines(integration_controller, config_obj=None) -> dict[str, Any]:
    configured_storage = get_knowledge_base_storage_config(config_obj)

    fallback_storage = []
    if not configured_storage and os.environ.get("KB_PGVECTOR_URL"):
        fallback_storage = ["pgvector"]

    candidate_storage = configured_storage or fallback_storage
    resolved_storage = []
    default_engine = None

    for engine in candidate_storage:
        handler_meta = integration_controller.get_handler_meta(engine)
        available = bool(handler_meta and handler_meta["import"]["success"])

        if available and default_engine is None:
            default_engine = engine

        resolved_storage.append(
            {
                "engine": engine,
                "available": available,
                "default": available and engine == default_engine,
                "source": "config" if engine in configured_storage else "fallback",
            }
        )

    return {
        "storage": configured_storage,
        "resolved_storage": resolved_storage,
        "default_storage": default_engine,
    }

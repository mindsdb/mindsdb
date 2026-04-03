import os
from types import SimpleNamespace
from unittest.mock import MagicMock
from unittest.mock import patch

from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
from mindsdb.interfaces.knowledge_base.default_storage_resolver import resolve_default_storage_engines
from mindsdb.utilities.config import config


def _make_controller(handler_meta_by_name):
    integration_controller = MagicMock()
    integration_controller.get_handler_meta.side_effect = lambda name: handler_meta_by_name.get(name)
    integration_controller.get.return_value = None

    session = SimpleNamespace(integration_controller=integration_controller)
    return KnowledgeBaseController(session), integration_controller


def test_resolve_default_vector_storage_uses_pgvector_from_config():
    previous_storage = config["knowledge_bases"].get("storage", None)
    controller, _ = _make_controller({"pgvector": {"import": {"success": True}}})

    try:
        config.update({"knowledge_bases": {"storage": "pgvector"}})
        vector_db_name = "kb_pgvector_store"
        controller._create_persistent_pgvector = MagicMock(return_value=vector_db_name)

        vector_db, vector_table = controller._resolve_default_vector_storage("kb_docs")

        assert vector_db == vector_db_name
        assert vector_table == "kb_docs"
        controller._create_persistent_pgvector.assert_called_once_with({})
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})


def test_resolve_default_vector_storage_uses_faiss_from_config():
    previous_storage = config["knowledge_bases"].get("storage", None)
    controller, _ = _make_controller({"duckdb_faiss": {"import": {"success": True}}})

    try:
        config.update({"knowledge_bases": {"storage": "faiss"}})

        vector_db_name = "store_kb_docs"
        controller._create_persistent_faiss = MagicMock(return_value=vector_db_name)

        vector_db, vector_table = controller._resolve_default_vector_storage("kb_docs")

        assert vector_db == vector_db_name
        assert vector_table == "kb_docs"
        controller._create_persistent_faiss.assert_called_once_with("kb_docs")
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})


def test_create_persistent_pgvector_reuses_existing_store():
    controller, integration_controller = _make_controller({})
    integration_controller.get.return_value = {"name": "kb_pgvector_store"}

    vector_store_name = controller._create_persistent_pgvector({"is_sparse": True, "vector_size": 30522})

    assert vector_store_name == "kb_pgvector_store"
    integration_controller.add.assert_not_called()


def test_resolver_uses_pgvector_url_fallback_when_storage_is_empty():
    previous_storage = config["knowledge_bases"].get("storage", None)
    controller, _ = _make_controller({})

    try:
        config.update({"knowledge_bases": {"storage": None}})
        with patch.dict(os.environ, {"KB_PGVECTOR_URL": "postgresql://user:pass@host/db"}, clear=False):
            resolved = resolve_default_storage_engines(config)
            assert resolved["default_storage"] == "pgvector"
            assert resolved["available_vector_engines"] == ["faiss", "pgvector"]
            assert resolved["pgvector_enabled"] is True
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})

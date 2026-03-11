from types import SimpleNamespace
from unittest.mock import MagicMock

from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController
from mindsdb.utilities.config import config


def _make_controller(handler_meta_by_name):
    integration_controller = MagicMock()
    integration_controller.get_handler_meta.side_effect = lambda name: handler_meta_by_name.get(name)
    integration_controller.get.return_value = None

    session = SimpleNamespace(integration_controller=integration_controller)
    return KnowledgeBaseController(session), integration_controller


def test_resolve_default_vector_storage_uses_pgvector_from_config():
    previous_storage = config["knowledge_bases"].get("storage", [])
    controller, integration_controller = _make_controller({"pgvector": {"import": {"success": True}}})

    try:
        config.update({"knowledge_bases": {"storage": ["pgvector"]}})

        vector_db_name = "kb_pgvector_store"
        controller._create_persistent_pgvector = MagicMock(return_value=vector_db_name)

        vector_db, vector_table = controller._resolve_default_vector_storage("kb_docs")

        assert vector_db == vector_db_name
        assert vector_table == "kb_docs"
        controller._create_persistent_pgvector.assert_called_once_with({})
        integration_controller.get_handler_meta.assert_called_with("pgvector")
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})


def test_resolve_default_vector_storage_uses_faiss_from_config():
    previous_storage = config["knowledge_bases"].get("storage", [])
    controller, integration_controller = _make_controller({"duckdb_faiss": {"import": {"success": True}}})

    try:
        config.update({"knowledge_bases": {"storage": ["duckdb_faiss"]}})

        vector_db_name = "kb_docs_duckdb_faiss"
        controller._create_persistent_chroma = MagicMock(return_value=vector_db_name)

        vector_db, vector_table = controller._resolve_default_vector_storage("kb_docs")

        assert vector_db == vector_db_name
        assert vector_table == "kb_docs"
        controller._create_persistent_chroma.assert_called_once_with("kb_docs", engine="duckdb_faiss")
        integration_controller.get_handler_meta.assert_called_with("duckdb_faiss")
    finally:
        config.update({"knowledge_bases": {"storage": previous_storage}})

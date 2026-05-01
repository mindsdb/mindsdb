"""Tests for KB storage handler validation (gh-11910).

Before this fix, ``KnowledgeBaseController.add`` happily accepted any
integration as ``storage`` and only blew up later inside
``vector_store_handler.create_table(...)`` with an opaque
``AttributeError: 'FileHandler' object has no attribute 'create_table'``.
The user has no way to tell from that message that they need to point
``storage`` at a vector database.

This test pins the friendly, up-front ``ValueError`` so the bad message
cannot regress.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from mindsdb_sql_parser.ast import Identifier

from mindsdb.integrations.libs.vectordatabase_handler import VectorStoreHandler
from mindsdb.interfaces.knowledge_base.controller import KnowledgeBaseController


class _NotAVectorStore:
    """Stand-in for an integration handler that is not a vector database
    (e.g. a FileHandler or a SQL handler)."""


class _FakeVectorStore(VectorStoreHandler):
    """Concrete subclass so isinstance(_, VectorStoreHandler) is True."""

    def __init__(self):  # pragma: no cover - trivial
        # Intentionally bypass BaseHandler.__init__ — we only need the type.
        pass


def _build_controller_with_storage(handler):
    """Wire up just enough of the controller to reach the storage validation."""
    project = SimpleNamespace(id=1, name="mindsdb")
    database_controller = MagicMock()
    database_controller.get_project.return_value = project

    data_node = SimpleNamespace(integration_handler=handler)
    datahub = MagicMock()
    datahub.get.return_value = data_node

    integration_controller = MagicMock()
    integration_controller.get.return_value = {"id": 42}

    session = SimpleNamespace(
        database_controller=database_controller,
        datahub=datahub,
        integration_controller=integration_controller,
        model_controller=MagicMock(),
    )
    return KnowledgeBaseController(session)


def _embedding_params():
    return {
        "embedding_model": {
            "provider": "openai",
            "model_name": "text-embedding-3-small",
            "api_key": "sk-test",
        }
    }


def test_non_vector_storage_raises_clear_error_before_create_table():
    """A non-vector handler must be rejected with an actionable message."""
    controller = _build_controller_with_storage(_NotAVectorStore())

    with (
        patch.object(
            KnowledgeBaseController,
            "_check_embedding_model",
            return_value={"dimension": 1536},
        ),
        patch.object(KnowledgeBaseController, "get", return_value=None),
    ):
        with pytest.raises(ValueError) as exc_info:
            controller.add(
                name="kb_bad_storage",
                project_name="mindsdb",
                storage=Identifier(parts=["files", "test_knowledge1"]),
                params=_embedding_params(),
            )

    message = str(exc_info.value)
    assert "files" in message
    assert "vector database" in message
    assert "pgvector" in message  # message must point user at valid alternatives
    # The original AttributeError leak must not be what the user sees.
    assert "create_table" not in message
    assert "_NotAVectorStore" in message


def test_vector_storage_passes_validation():
    """A real VectorStoreHandler subclass is accepted (proves the guard
    is not over-eager)."""
    handler = _FakeVectorStore()
    handler.create_table = MagicMock()
    handler.drop_table = MagicMock()
    handler.add_full_text_index = MagicMock()
    controller = _build_controller_with_storage(handler)

    with (
        patch.object(
            KnowledgeBaseController,
            "_check_embedding_model",
            return_value={"dimension": 1536},
        ),
        patch.object(KnowledgeBaseController, "_check_vector_table", return_value=None),
        patch.object(KnowledgeBaseController, "get", return_value=None),
        patch("mindsdb.interfaces.knowledge_base.controller.db.session"),
    ):
        # We don't care about the return value here; we only care that the
        # storage-type guard does not reject a real vector handler. Any
        # downstream failure should NOT be the ValueError we are protecting
        # against in the negative test above.
        try:
            controller.add(
                name="kb_good_storage",
                project_name="mindsdb",
                storage=Identifier(parts=["pgvector_db", "kb_table"]),
                params=_embedding_params(),
            )
        except ValueError as e:
            assert "is not a vector database" not in str(e)

    handler.create_table.assert_called_once_with("kb_table")

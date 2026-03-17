"""
Unit tests for MCP resources (mindsdb/api/mcp/resources/*)
"""

import asyncio
import json
from unittest.mock import MagicMock, patch

import pandas as pd

from mindsdb.integrations.libs.response import TableResponse as HandlerTableResponse
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


_PATCH_SESSION = "mindsdb.api.mcp.resources.schema.SessionController"
_PATCH_TABLE_RESPONSE = "mindsdb.api.mcp.resources.schema.TableResponse"
_PATCH_RESPONSE_TYPE = "mindsdb.api.mcp.resources.schema.RESPONSE_TYPE"


def _run(coro):
    return asyncio.run(coro)


def _read(uri: str) -> list:
    """Read a resource and return parsed JSON payload."""
    from mindsdb.api.mcp.mcp_instance import mcp

    contents = list(_run(mcp.read_resource(uri)))
    return json.loads(contents[0].content)


def _make_table_mock(name: str, table_type: str = "BASE TABLE", schema: str = "public") -> MagicMock:
    t = MagicMock()
    t.TABLE_NAME = name
    t.TABLE_TYPE = table_type
    t.TABLE_SCHEMA = schema
    return t


def _make_columns_table_response(rows: list[dict]) -> MagicMock:
    """Build a mock HandlerTableResponse with COLUMNS_TABLE type."""
    tr = MagicMock(spec=HandlerTableResponse)
    tr.type = RESPONSE_TYPE.COLUMNS_TABLE
    tr.fetchall.return_value = pd.DataFrame(rows)
    return tr


def _make_kb(name, project, metadata_cols=None, content_cols=None, id_col="id"):
    return {
        "name": name,
        "project": project,
        "metadata_columns": metadata_cols or [],
        "content_columns": content_cols or ["body"],
        "id_column": id_col,
    }


class TestListDatabases:
    def test_returns_only_data_type_databases(self):
        from mindsdb.api.mcp.mcp_instance import mcp

        with patch(_PATCH_SESSION) as SC:
            SC.return_value.database_controller.get_list.return_value = [
                {"name": "pg_prod", "type": "data"},
                {"name": "mindsdb", "type": "project"},
                {"name": "mysql_db", "type": "data"},
            ]

            result = list(_run(mcp.read_resource("schema://databases")))

        assert len(result) == 1
        assert json.loads(result[0].content) == ["pg_prod", "mysql_db"]
        assert result[0].mime_type == "application/json"

    def test_filters_out_all_non_data_types(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.database_controller.get_list.return_value = [
                {"name": "mindsdb", "type": "project"},
                {"name": "files", "type": "files"},
            ]
            result = _read("schema://databases")

        assert result == []


class TestDbTables:
    def test_returns_table_names(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get.return_value.get_tables.return_value = [
                _make_table_mock("orders"),
                _make_table_mock("users"),
            ]
            result = _read("schema://databases/mydb/tables")

            SC.return_value.datahub.get.assert_called_once_with("mydb")

        names = [t["TABLE_NAME"] for t in result]
        assert names == ["orders", "users"]
        assert set(result[0].keys()) == {"TABLE_NAME", "TABLE_TYPE", "TABLE_SCHEMA"}

    def test_returns_table_type_and_schema(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get.return_value.get_tables.return_value = [
                _make_table_mock("orders", table_type="VIEW", schema="myschema"),
            ]
            result = _read("schema://databases/mydb/tables")

        assert result[0]["TABLE_TYPE"] == "VIEW"
        assert result[0]["TABLE_SCHEMA"] == "myschema"

    def test_empty_database_returns_empty_list(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get.return_value.get_tables.return_value = []
            result = _read("schema://databases/emptydb/tables")

        assert result == []


class TestDbTableColumns:
    def test_returns_column_names_and_types(self):
        rows = [
            {"COLUMN_NAME": "id", "MYSQL_DATA_TYPE": "int"},
            {"COLUMN_NAME": "email", "MYSQL_DATA_TYPE": "varchar(255)"},
        ]
        with (
            patch(_PATCH_SESSION) as SC,
            patch(_PATCH_TABLE_RESPONSE, HandlerTableResponse),
            patch(_PATCH_RESPONSE_TYPE, RESPONSE_TYPE),
        ):
            SC.return_value.integration_controller.get_data_handler.return_value.get_columns.return_value = (
                _make_columns_table_response(rows)
            )

            result = _read("schema://databases/mydb/tables/orders/columns")
            SC.return_value.integration_controller.get_data_handler.assert_called_once_with("mydb")
            SC.return_value.integration_controller.get_data_handler.return_value.get_columns.assert_called_once_with(
                "orders"
            )

        assert result[0] == {"COLUMN_NAME": "id", "MYSQL_DATA_TYPE": "int"}
        assert result[1] == {"COLUMN_NAME": "email", "MYSQL_DATA_TYPE": "varchar(255)"}


class TestListKnowledgeBases:
    def test_returns_knowledge_bases_from_all_projects(self):
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get_projects_names.return_value = ["mindsdb", "my_project"]
            SC.return_value.kb_controller.list.side_effect = [
                [_make_kb("kb1", "mindsdb")],
                [_make_kb("kb2", "my_project")],
            ]
            result = _read("schema://knowledge_bases")

        assert len(result) == 2
        assert result[0]["name"] == "kb1"
        assert result[1]["name"] == "kb2"

    def test_returns_correct_kb_fields(self):
        kb = _make_kb(
            "docs_kb",
            "mindsdb",
            metadata_cols=["source", "date"],
            content_cols=["body"],
            id_col="doc_id",
        )
        with patch(_PATCH_SESSION) as SC:
            SC.return_value.datahub.get_projects_names.return_value = ["mindsdb"]
            SC.return_value.kb_controller.list.return_value = [kb]
            result = _read("schema://knowledge_bases")

        assert result[0] == {
            "name": "docs_kb",
            "project": "mindsdb",
            "metadata_columns": ["source", "date"],
            "content_columns": ["body"],
            "id_column": "doc_id",
        }

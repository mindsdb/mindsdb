from pydantic import BaseModel

from mindsdb.api.mcp.mcp_instance import mcp
from mindsdb.api.executor.controllers.session_controller import SessionController
from mindsdb.utilities.context import context as ctx
from mindsdb.integrations.libs.response import TableResponse, ErrorResponse
from mindsdb.api.executor.data_types.response_type import RESPONSE_TYPE


class TableInfo(BaseModel):
    TABLE_NAME: str
    TABLE_TYPE: str
    TABLE_SCHEMA: str


class ColumnInfo(BaseModel):
    COLUMN_NAME: str
    MYSQL_DATA_TYPE: str


class KnowledgeBaseInfo(BaseModel):
    name: str
    project: str
    metadata_columns: list[str]
    content_columns: list[str]
    id_column: str


def _get_database_names() -> list[str]:
    ctx.set_default()
    session = SessionController()
    databases = session.database_controller.get_list()
    return [x["name"] for x in databases if x["type"] == "data"]


@mcp.resource(
    "schema://databases",
    mime_type="application/json",
    description=(
        "Initial list of connected data source names available for querying. "
        "This resource may be cached by the client. "
        "To get the current list of databases during a session, use the `query` tool: "
        "SHOW DATABASES"
    ),
)
def list_databases() -> list[str]:
    return _get_database_names()


@mcp.resource(
    "schema://databases/{database_name}/tables",
    mime_type="application/json",
    description=(
        "Initial list of tables in the specified connected database. "
        "This resource may be cached by the client. "
        "To get the current list of tables during a session (e.g. after CREATE/DROP TABLE), "
        "use the `query` tool: "
        "SHOW TABLES FROM {database_name}"
    ),
)
def db_tables(database_name: str) -> list[TableInfo]:
    ctx.set_default()
    session = SessionController()
    datanode = session.datahub.get(database_name)
    if datanode is None:
        raise ValueError(f"Database '{database_name}' is not found.")
    all_tables = datanode.get_tables()
    all_tables = [
        {
            "TABLE_NAME": table.TABLE_NAME,
            "TABLE_TYPE": table.TABLE_TYPE,
            "TABLE_SCHEMA": table.TABLE_SCHEMA,
        }
        for table in all_tables
    ]
    return all_tables


@mcp.resource(
    "schema://databases/{database_name}/tables/{table_name}/columns",
    mime_type="application/json",
    description=(
        "Initial column names and types for a specific table in a connected database. "
        "This resource may be cached by the client. "
        "To get the current column list during a session (e.g. after ALTER TABLE), "
        "use the `query` tool: "
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = '{database_name}' AND TABLE_NAME = '{table_name}'"
    ),
)
def db_table_columns(database_name: str, table_name: str) -> list[ColumnInfo]:
    ctx.set_default()
    session = SessionController()
    handler = session.integration_controller.get_data_handler(database_name)
    columns_answer = handler.get_columns(table_name)

    if isinstance(columns_answer, TableResponse):
        if columns_answer.type != RESPONSE_TYPE.COLUMNS_TABLE:
            raise ValueError(
                "Database returned a successful response, but the column list does not match the expected format"
            )
        df = columns_answer.fetchall()
        response = df[["COLUMN_NAME", "MYSQL_DATA_TYPE"]].to_dict(orient="records")
        return response
    if isinstance(columns_answer, ErrorResponse):
        raise ValueError(columns_answer.error_message)
    raise ValueError(f"Unexpected handler response type: {columns_answer}")


@mcp.resource(
    "schema://knowledge_bases",
    description=(
        "Initial list of knowledge bases with their project, column configuration, and ID column. "
        "This resource may be cached by the client. "
        "To get the current list of knowledge bases during a session, use the `query` tool: "
        "SHOW KNOWLEDGE BASES"
    ),
)
def list_knowledge_bases() -> list[KnowledgeBaseInfo]:
    ctx.set_default()
    session = SessionController()
    project_names = session.datahub.get_projects_names()
    result = []
    for project_name in project_names:
        kbs = session.kb_controller.list(project_name)
        for kb in kbs:
            result.append(
                {
                    "name": kb.get("name"),
                    "project": kb.get("project"),
                    "metadata_columns": kb.get("metadata_columns"),
                    "content_columns": kb.get("content_columns"),
                    "id_column": kb.get("id_column"),
                }
            )
    return result

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


@mcp.resource("schema://databases", mime_type="application/json")
def list_databases() -> list[str]:
    return _get_database_names()


@mcp.resource("schema://databases/{database_name}/tables", mime_type="application/json")
def db_tables(database_name: str) -> list[TableInfo]:
    ctx.set_default()
    session = SessionController()
    datanode = session.datahub.get(database_name)
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


@mcp.resource("schema://databases/{database_name}/tables/{table_name}/columns", mime_type="application/json")
def db_table_columns(database_name: str, table_name: str) -> list[ColumnInfo]:
    ctx.set_default()
    session = SessionController()
    handler = session.integration_controller.get_data_handler(database_name)
    columns_answer = handler.get_columns(table_name)
    respnse = []
    if isinstance(columns_answer, TableResponse) and columns_answer.type == RESPONSE_TYPE.COLUMNS_TABLE:
        if columns_answer.type != RESPONSE_TYPE.COLUMNS_TABLE:
            raise ValueError(
                "Database returned a successful response, but the column list does not match the expected format"
            )
        df = columns_answer.fetchall()
        respnse = df[["COLUMN_NAME", "MYSQL_DATA_TYPE"]].to_dict(orient="records")
        return respnse
    if isinstance(columns_answer, ErrorResponse):
        raise ValueError(columns_answer.error_message)


@mcp.resource("schema://knowledge_bases")
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
                    "name": kb["name"],
                    "project": kb["project"],
                    "metadata_columns": kb["metadata_columns"],
                    "content_columns": kb["content_columns"],
                    "id_column": kb["id_column"],
                }
            )
    return result

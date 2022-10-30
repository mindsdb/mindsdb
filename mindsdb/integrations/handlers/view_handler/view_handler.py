import copy

from pandas import DataFrame

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Identifier

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)


class ViewHandler(DatabaseHandler):
    """
    This handler handles views
    """

    name = 'views'

    def __init__(self, name=None, **kwargs):
        self.view_controller = kwargs['view_controller']
        super().__init__(name)

    def check_connection(self) -> StatusResponse:
        """
        Check the connection of the PostgreSQL database
        :return: success status and error message if error occurs
        """
        return StatusResponse(True)

    def native_query(self, query: str) -> Response:

        ast = parse_sql(query, dialect='mindsdb')

        return self.query(ast)

    def query(self, query: ASTNode, db_name: str = None) -> Response:
        view_name = query.from_table.parts[-1]
        if query.from_table.alias is not None:
            view_alias = query.from_table.alias.parts[-1]
        else:
            view_alias = view_name
        if db_name is None:
            if len(query.from_table.parts) == 2:
                db_name = query.from_table.parts[0]
            else:
                db_name = 'mindsdb'
        view_meta = self.view_controller.get(name=view_name, project_name=db_name)

        subquery_ast = parse_sql(view_meta['query'], dialect='mindsdb')

        return Response(
            RESPONSE_TYPE.QUERY,
            query=subquery_ast
        )

    def get_tables(self) -> Response:
        """
        List all views
        """
        views = self.view_controller.get_all()
        result = []
        for view_name, view_meta in views.items():
            result.append({
                'table_name': view_meta['name'],
                'table_type': 'VIEW'
            })
        response = Response(
            RESPONSE_TYPE.TABLE,
            DataFrame(result)
        )
        return response

    def get_columns(self, table_name):
        # TODO
        query = f"""
            SELECT
                column_name as "Field",
                data_type as "Type"
            FROM
                information_schema.columns
            WHERE
                table_name = '{table_name}'
        """
        return self.native_query(query)

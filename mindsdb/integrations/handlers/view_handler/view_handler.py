import copy

from pandas import DataFrame

from mindsdb_sql import parse_sql
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Identifier

from mindsdb.integrations.libs.base_handler import DatabaseHandler
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

    def query(self, query: ASTNode) -> Response:

        view_name = query.from_table.parts[-1]
        view_meta = self.view_controller.get(name=view_name)

        subquery_ast = parse_sql(view_meta['query'], dialect='mindsdb')
        if query.from_table.parts[-1] != view_name:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Query does not contain view name '{view_name}': {query}"
            )

        # set alias
        query = copy.deepcopy(query)
        subquery_ast.alias = Identifier(view_name)
        query.from_table = subquery_ast

        return Response(
            RESPONSE_TYPE.QUERY,
            query=query
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

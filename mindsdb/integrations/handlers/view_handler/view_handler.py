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

    def native_query(self, query: ASTNode) -> Response:
        """
        Receive SQL query and runs it
        :param query: The SQL query to run in PostgreSQL
        :return: returns the records from the current recordset
        """
        view_name = query.from_table.parts[-1]
        view_meta = self.view_controller.get(name=view_name)

        subquery_ast = parse_sql(view_meta['query'], dialect='mysql')
        if query.from_table.parts[-1] != view_name:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Query does not contain view name '{view_name}': {query}"
            )

        # set alias
        subquery_ast.alias = Identifier(view_name)
        query.from_table = subquery_ast

        return Response(
            RESPONSE_TYPE.QUERY,
            query=query
        )

    def query(self, query: ASTNode) -> Response:
        """
        Retrieve the data from the SQL statement with eliminated rows that dont satisfy the WHERE condition
        """
        return self.native_query(query)
        # query_str = self.renderer.get_string(query, with_failback=True)
        # return self.native_query(query_str)

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

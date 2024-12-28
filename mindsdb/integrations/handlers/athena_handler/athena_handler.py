import time
import pandas as pd
from boto3 import client
from typing import Optional

from mindsdb_sql_parser import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql_parser.ast.base import ASTNode
from mindsdb.utilities import log
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)

logger = log.getLogger(__name__)


class AthenaHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the Athena statements.
    """

    name = 'athena'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
        """
        Initialize the handler.
        Args:
            name (str): name of particular handler instance
            connection_data (dict): parameters for connecting to the database
            **kwargs: arbitrary keyword arguments.
        """
        super().__init__(name)
        self.parser = parse_sql
        self.dialect = 'athena'

        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> StatusResponse:
        """
        Set up the connection required by the handler.
        Returns:
            HandlerStatusResponse
        """

        if self.is_connected:
            return StatusResponse(success=True)

        try:
            self.connection = client(
                'athena',
                aws_access_key_id=self.connection_data['aws_access_key_id'],
                aws_secret_access_key=self.connection_data['aws_secret_access_key'],
                region_name=self.connection_data['region_name'],
            )
            self.is_connected = True
            return StatusResponse(success=True)
        except Exception as e:
            logger.error(f'Failed to connect to Athena: {str(e)}')
            return StatusResponse(success=False, error_message=str(e))

    def disconnect(self):
        """
        Close any existing connections.
        """
        if self.is_connected:
            self.connection = None
            self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """
        if self.is_connected:
            return StatusResponse(success=True)
        else:
            return self.connect()

    def native_query(self, query: str) -> Response:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """
        need_to_close = not self.is_connected
        self.connect()

        try:
            response = self.connection.start_query_execution(
                QueryString=query,
                QueryExecutionContext={
                    'Database': self.connection_data['database'],
                },
                ResultConfiguration={
                    'OutputLocation': self.connection_data['results_output_location'],
                },
                WorkGroup=self.connection_data['workgroup'],
            )
            query_execution_id = response['QueryExecutionId']
            status = self._wait_for_query_to_complete(query_execution_id)
            if status == 'SUCCEEDED':
                result = self.connection.get_query_results(
                    QueryExecutionId=query_execution_id
                )
                df = self._parse_query_result(result)
                response = Response(RESPONSE_TYPE.TABLE, data_frame=df)
            else:
                response = Response(RESPONSE_TYPE.ERROR, error_message='Query failed or was cancelled')
        except Exception as e:
            logger.error(f'Error executing query in Athena: {str(e)}')
            response = Response(RESPONSE_TYPE.ERROR, error_message=str(e))

        if need_to_close:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            Response: A response object containing the list of tables and
        """

        query = """
            select
                table_schema,
                table_name,
                table_type
            from
                information_schema.tables
            where
                table_schema not in ('information_schema')
            and table_type in ('BASE TABLE', 'VIEW')
        """
        return self.native_query(query)

    def get_columns(self, table_name: str) -> Response:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            Response: A response object containing the column details
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid value for table name provided.")

        query = f"""
            select
                column_name as "Field",
                data_type as "Type"
            from
                information_schema.columns
            where
                table_name = '{table_name}'
        """
        return self.native_query(query)

    def _wait_for_query_to_complete(self, query_execution_id: str) -> str:
        """
        Wait for the Athena query to complete.
        Args:
            query_execution_id (str): ID of the query to wait for
        Returns:
            str: Query execution status
        """
        while True:
            response = self.connection.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status

            check_interval = self.connection_data.get('check_interval', 0)
            if isinstance(check_interval, str) and check_interval.strip().isdigit():
                check_interval = int(check_interval)
            if check_interval > 0:
                time.sleep(check_interval)

    def _parse_query_result(self, result: dict) -> pd.DataFrame:
        """
        Parse the result of the Athena query into a DataFrame.
        Args:
            result: Result of the Athena query
        Returns:
            pd.DataFrame: Query result as a DataFrame
        """

        if not result or 'ResultSet' not in result or 'Rows' not in result['ResultSet']:
            return pd.DataFrame()

        rows = result['ResultSet']['Rows']
        headers = [col['VarCharValue'] for col in rows[0]['Data']]
        data = [[col.get('VarCharValue') for col in row['Data']] for row in rows[1:]]
        return pd.DataFrame(data, columns=headers)

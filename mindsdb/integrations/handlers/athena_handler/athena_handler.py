import time
import pandas as pd
from boto3 import client
from typing import Optional

from mindsdb_sql import parse_sql
from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb_sql.parser.ast.base import ASTNode
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

        if self.is_connected is True:
            return self.connection

        self.connection = client(
            'athena',
            aws_access_key_id=self.connection_data['aws_access_key_id'],
            aws_secret_access_key=self.connection_data['aws_secret_access_key'],
            region_name=self.connection_data['region_name']
        )

        self.is_connected = True

        return self.connection

    def disconnect(self):
        """
        Close any existing connections.
        """

        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False
        return self.is_connected

    def check_connection(self) -> StatusResponse:
        """
        Check connection to the handler.
        Returns:
            HandlerStatusResponse
        """

        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            self.connect()
            response.success = True
        except Exception as e:
            logger.error(f'Error connecting to Athena, {e}!')
            response.error_message = str(e)
        finally:
            if response.success is True and need_to_close:
                self.disconnect()
            if response.success is False and self.is_connected is True:
                self.is_connected = False

        return response

    def native_query(self, query: str) -> StatusResponse:
        """
        Receive raw query and act upon it somehow.
        Args:
            query (str): query in native format
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            # Start the query execution
            response = connection.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': self.connection_data['results_output_location'],  # specify the S3 path where query results will be stored
                }
            )

            # Get the query execution id
            query_execution_id = response['QueryExecutionId']

            # Wait for the query to finish execution
            while True:
                response = connection.get_query_execution(QueryExecutionId=query_execution_id)

                if response['QueryExecution']['Status']['State'] in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break

                time.sleep(5)  # wait for 5 seconds before checking the query status again

            # If query completed successfully, fetch the results
            if response['QueryExecution']['Status']['State'] == 'SUCCEEDED':
                result = connection.get_query_results(QueryExecutionId=query_execution_id)

                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.json_normalize(result['ResultSet']['Rows'])
                )
            else:
                logger.error(f'Error running query: {query} on Athena!')
                response = Response(
                    RESPONSE_TYPE.ERROR,
                    error_message=response['QueryExecution']['Status']['StateChangeReason']
                )

        except Exception as e:
            logger.error(f'Error running query: {query} on Athena!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> StatusResponse:
        """
        Receive query as AST (abstract syntax tree) and act upon it somehow.
        Args:
            query (ASTNode): sql query represented as AST. May be any kind
                of query: SELECT, INTSERT, DELETE, etc
        Returns:
            HandlerResponse
        """

        return self.native_query(query.to_string())

    def get_tables(self) -> StatusResponse:
        """
        Return list of entities that will be accessible as tables.
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            # Get the list of tables in the specified database
            response = connection.list_table_metadata(
                Catalog=self.connection_data['catalog'],
                DatabaseName=self.connection_data['database']
            )

            # Extract the table names from the response
            table_names = [table['Name'] for table in response['TableMetadataList']]

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(table_names, columns=['Table Name'])
            )

        except Exception as e:
            logger.error(f'Error getting tables from Athena!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def get_columns(self, table_name: str) -> StatusResponse:
        """
        Returns a list of entity columns.
        Args:
            table_name (str): name of one of tables returned by self.get_tables()
        Returns:
            HandlerResponse
        """

        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            # Get the table metadata
            response = connection.get_table_metadata(
                Catalog=self.connection_data['catalog'],
                DatabaseName=self.connection_data['database'],
                TableName=table_name
            )

            # Extract the column names from the response
            column_names = [column['Name'] for column in response['TableMetadata']['Columns']]

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=pd.DataFrame(column_names, columns=['Column Name'])
            )

        except Exception as e:
            logger.error(f'Error getting columns from table {table_name} in Athena!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response

from typing import Text, List, Dict, Optional

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, AuthenticationException, TransportError, RequestError
from elasticsearch.helpers import bulk
from elasticsearch.helpers.errors import BulkIndexError
from es.elastic.sqlalchemy import ESDialect
from pandas import DataFrame
from mindsdb_sql.parser.ast import Update
from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.render.sqlalchemy_render import SqlalchemyRender

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class ElasticsearchHandler(DatabaseHandler):
    """
    This handler handles the connection and execution of SQL statements on Elasticsearch.
    """

    name = 'elasticsearch'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the AWS (S3) account.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def __del__(self) -> None:
        """
        Closes the connection when the handler instance is deleted.
        """
        if self.is_connected:
            self.disconnect()

    def connect(self) -> Elasticsearch:
        """
        Establishes a connection to the Elasticsearch host.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            elasticsearch.Elasticsearch: A connection object to the Elasticsearch host.
        """
        if self.is_connected is True:
            return self.connection

        config = {}

        # Mandatory connection parameters.
        if ('hosts' not in self.connection_data) and ('cloud_id' not in self.connection_data):
            raise ValueError("Either the hosts or cloud_id parameter should be provided!")

        # Optional/Additional connection parameters.
        optional_parameters = ['hosts', 'cloud_id', 'api_key']
        for parameter in optional_parameters:
            if parameter in self.connection_data:
                if parameter == 'hosts':
                    config['hosts'] = self.connection_data[parameter].split(',')
                else:
                    config[parameter] = self.connection_data[parameter]

        # Ensure that if either user or password is provided, both are provided.
        if ('user' in self.connection_data) != ('password' in self.connection_data):
            raise ValueError("Both user and password should be provided if one of them is provided!")

        if 'user' in self.connection_data:
            config['http_auth'] = (self.connection_data['user'], self.connection_data['password'])

        try:
            self.connection = Elasticsearch(
                **config,
            )
            self.is_connected = True
            return self.connection
        except ConnectionError as conn_error:
            logger.error(f'Connection error when connecting to Elasticsearch: {conn_error}')
            raise
        except AuthenticationException as auth_error:
            logger.error(f'Authentication error when connecting to Elasticsearch: {auth_error}')
            raise
        except Exception as unknown_error:
            logger.error(f'Unknown error when connecting to Elasticsearch: {unknown_error}')
            raise

    def disconnect(self) -> None:
        """
        Closes the connection to the Elasticsearch host if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Elasticsearch host.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()

            # Execute a simple query to test the connection.
            connection.sql.query(body={'query': 'SELECT 1'})
            response.success = True
        # All exceptions are caught here to ensure that the connection is closed if an error occurs.
        except Exception as error:
            logger.error(f'Error connecting to Elasticsearch, {error}!')
            response.error_message = str(error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query on the Elasticsearch host and returns the result.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()
        try:
            response = connection.sql.query(body={'query': query})
            records = response['rows']
            columns = response['columns']

            new_records = True
            while new_records:
                try:
                    if response['cursor']:
                        response = connection.sql.query(body={'query': query, 'cursor': response['cursor']})

                        new_records = response['rows']
                        records = records + new_records
                except KeyError:
                    new_records = False

            if records:
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=DataFrame(
                        records,
                        columns=[column['name'] for column in columns]
                    )
                )
        except (TransportError, RequestError) as transport_or_request_error:
            logger.error(f'Error running query: {query} on Elasticsearch, {transport_or_request_error}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(transport_or_request_error)
            )
        except Exception as unknown_error:
            logger.error(f'Unknown error running query: {query} on Elasticsearch, {unknown_error}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(unknown_error)
            )

        if need_to_close is True:
            self.disconnect()

        return response
    
    def insert(self, table_name: Text, df: DataFrame) -> Response:
        """
        Executes an update query on the Elasticsearch host.
        This function will be invoked instead of `native_query` when the query is an insert statement.

        Args:
            table_name (str): The name of the table (index) to insert the data into.
            df (DataFrame): The data to be inserted into the table.

        Returns:
            Response: A response object containing the result of the insert operation.
        """
        # Add the index name and the 'create' operation type to each document.
        df['_index'] = table_name
        df['_op_type'] = 'create'

        documents = df.to_dict(orient='records')
                    
        return self._index_in_bulk(documents)
    
    def update(self, query: ASTNode) -> Response:
        """
        Executes an update query on the Elasticsearch host.

        Args:
            query (ASTNode): An ASTNode representing the SQL update query to be executed.

        Returns:
            Response: A response object containing the result of the update operation.
        """
        where_conditions = extract_comparison_conditions(query.where)

        # Validate if a WHERE clause is provided with an _id filter.
        if not where_conditions or len(where_conditions) != 1 or where_conditions[0][1] != '_id':
            raise ValueError("A WHERE clause with an _id filter is required for an update operation.")
        
        if where_conditions[0][0] not in ['=', 'in']:
            raise ValueError("Only the '=' and 'in' operators are supported for the _id filter.")
        
        ids = where_conditions[0][2] if isinstance(where_conditions[0][2], list) else [where_conditions[0][2]]

        table_name = query.table.get_string()
        values_to_update = query.update_columns

        # Add the index name, _id and the 'update' operation type to each document.
        documents = []
        for _id in ids:
            document = {
                '_index': table_name,
                '_id': _id,
                '_op_type': 'update',
                '_source': {'doc': {key: val.value for key, val in values_to_update.items()}}
            }
            documents.append(document)

        return self._index_in_bulk(documents)
    
    def _index_in_bulk(self, documents: List[Dict]) -> Response:
        """
        Indexes a list of documents in bulk into the Elasticsearch host.
        Both inserts and updates are supported based on the '_op_type' field and the other fields in the document.

        Args:
            documents (List[Dict]): A list of documents to be inserted into the Elasticsearch host.

        Returns:
            Response: A response object containing the result of the bulk insert operation.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            bulk(connection, documents)
            response = Response(RESPONSE_TYPE.OK)
        except (BulkIndexError, RequestError) as bulk_index_or_request_error:
            logger.error(f'Error inserting data into Elasticsearch: {bulk_index_or_request_error}')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(bulk_index_or_request_error)
            )
        except Exception as unknown_error:
            logger.error(f'Unknown error inserting data into Elasticsearch: {unknown_error}')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(unknown_error)
            )

        if need_to_close is True:
            self.disconnect()

        return response

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on the Elasticsearch host and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        if isinstance(query, Update):
            return self.update(query)

        renderer = SqlalchemyRender(ESDialect)
        query_str = renderer.get_string(query, with_failback=True)
        logger.debug(f"Executing SQL query: {query_str}")
        return self.native_query(query_str)

    def get_tables(self) -> Response:
        """
        Retrieves a list of all non-system tables (indexes) in the Elasticsearch host.

        Returns:
            Response: A response object containing a list of tables (indexes) in the Elasticsearch host.
        """
        query = """
            SHOW TABLES
        """
        result = self.native_query(query)

        df = result.data_frame

        # Remove indices that are system indices: These are indices that start with a period.
        df = df[~df['name'].str.startswith('.')]

        df = df.drop(['catalog', 'kind'], axis=1)
        result.data_frame = df.rename(columns={'name': 'table_name', 'type': 'table_type'})

        return result

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column (field) details for a specified table (index) in the Elasticsearch host.

        Args:
            table_name (str): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"""
            DESCRIBE {table_name}
        """
        result = self.native_query(query)

        df = result.data_frame
        df = df.drop('mapping', axis=1)
        result.data_frame = df.rename(columns={'column': 'column_name', 'type': 'data_type'})

        return result

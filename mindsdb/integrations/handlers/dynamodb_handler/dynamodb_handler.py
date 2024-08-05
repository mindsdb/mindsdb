from typing import Text, List, Dict, Optional

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError
from mindsdb_sql.parser.ast.base import ASTNode
import pandas as pd

from mindsdb.integrations.libs.base import DatabaseHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class DyanmoDBHandler(DatabaseHandler):
    """
    This handler handles connection and execution of the SQL statements on Amazon DyanmoDB.
    """

    name = 'dynamodb'

    def __init__(self, name: Text, connection_data: Optional[Dict], **kwargs):
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to Amazon DyanmoDB.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False

    def connect(self) -> boto3.client:
        """
        Establishes a connection to Amazon DynamoDB.

        Raises:
            ValueError: If the expected connection parameters are not provided.

        Returns:
            boto3.client: A client object to Amazon DynamoDB.
        """
        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key', 'region_name']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key, region_name) must be provided.')
        
        config = {
            'aws_access_key_id': self.connection_data.get('aws_access_key_id'),
            'aws_secret_access_key': self.connection_data.get('aws_secret_access_key'),
            'region_name': self.connection_data.get('region_name')
        }

        # Optional connection parameters.
        optional_parameters = ['aws_session_token']
        for param in optional_parameters:
            if param in self.connection_data:
                config[param] = self.connection_data[param]

        # TODO: Add error handling.
        self.connection = boto3.client(
            'dynamodb',
            **config
        )

        self.is_connected = True

        return self.connection

    def disconnect(self) -> None:
        """
        Closes the connection to the Amazon DynamoDB if it's currently open.
        """
        if self.is_connected is False:
            return

        self.connection.close()
        self.is_connected = False

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to Amazon DynamoDB.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.list_tables()
            
            response.success = True
        except (ValueError, ClientError) as known_error:
            logger.error(f'Connection check to Amazon DynamoDB failed, {known_error}!')
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f'Connection check to Amazon DynamoDB failed due to an unknown error, {unknown_error}!')
            response.error_message = str(unknown_error)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SQL query (PartiQL) on Amazon DynamoDB and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        try:
            result = connection.execute_statement(Statement=query)
            if result['Items']:
                # TODO: Can parsing be optimized?
                records = []
                records.extend(self._parse_records(result['Items']))

                while 'LastEvaluatedKey' in result:
                    result = connection.execute_statement(
                        Statement=query,
                        NextToken=result['NextToken']
                    )
                    records.extend(self._parse_records(result['Items']))
                    
                response = Response(
                    RESPONSE_TYPE.TABLE,
                    data_frame=pd.json_normalize(records)
                )
            # TODO: Handle situations where a SELECT query returns no records.
            else:
                response = Response(RESPONSE_TYPE.OK)
        # TODO: Catch specific exceptions.
        except Exception as e:
            logger.error(f'Error running query: {query} on DynamoDB!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        connection.close()
        if need_to_close is True:
            self.disconnect()

        return response

    def _parse_records(self, records: List[Dict]) -> Dict:
        """
        Parses the records returned by the PartiQL query execution.

        Args:
            records (List[Dict]): A list of records returned by the PartiQL query execution.

        Returns:
            Dict: A dictionary containing the parsed record.
        """
        deserializer = TypeDeserializer()
        
        parsed_records = []
        for record in records:
            parsed_records.append({k: deserializer.deserialize(v) for k,v in record.items()})

        return parsed_records

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode on Amazon DynamoDB and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Retrieves a list of all tables in Amazon DynamoDB.

        Returns:
            Response: A response object containing a list of tables in Amazon DynamoDB.
        """
        result = self.connection.list_tables()

        df = pd.DataFrame(
            data=result['TableNames'],
            columns=['table_name']
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column (attribute) details for a specified table in Amazon DynamoDB.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Raises:
            ValueError: If the 'table_name' is not a valid string.

        Returns:
            Response: A response object containing the column details.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        result = self.connection.describe_table(
            TableName=table_name
        )

        df = pd.DataFrame(
            result['Table']['AttributeDefinitions']
        )

        df = df.rename(
            columns={
                'AttributeName': 'column_name',
                'AttributeType': 'data_type'
            }
        )

        response = Response(
            RESPONSE_TYPE.TABLE,
            df
        )

        return response

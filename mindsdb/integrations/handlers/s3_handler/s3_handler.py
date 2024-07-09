import json
import boto3
import pandas as pd
from typing import Text, Dict, Optional
from botocore.exceptions import ClientError

from mindsdb.utilities import log

from mindsdb_sql.parser.ast.base import ASTNode
from mindsdb_sql.parser.ast import Select, Identifier

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE
)
from mindsdb.integrations.libs.base import DatabaseHandler


logger = log.getLogger(__name__)

class S3Handler(DatabaseHandler):
    """
    This handler handles connection and execution of the S3 statements.
    """

    name = 's3'

    def __init__(self, name: str, connection_data: Optional[dict], **kwargs):
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
        self.table_name = None

        self.connection = None
        self.is_connected = False

    def __del__(self):
        if self.is_connected is True:
            self.disconnect()

    def connect(self) -> boto3.client:
        """
        Establishes a connection to the AWS (S3) account.

        Raises:
            KeyError: If the required connection parameters are not provided.
            
        Returns:
            boto3.client: A client object to the AWS (S3) account.
        """
        if self.is_connected is True:
            return self.connection
        
        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ['aws_access_key_id', 'aws_secret_access_key', 'bucket']):
            raise ValueError('Required parameters (aws_access_key_id, aws_secret_access_key, bucket) must be provided.')

        config = {
            'aws_access_key_id': self.connection_data.get('aws_access_key_id'),
            'aws_secret_access_key': self.connection_data.get('aws_secret_access_key'),
        }

        # Optional connection parameters.
        optional_params = ['aws_session_token', 'region_name']
        for param in optional_params:
            if param in self.connection_data:
                config[param] = self.connection_data[param]

        self.connection = boto3.client(
            's3',
            **config
        )
        self.is_connected = True

        return self.connection

    def disconnect(self) -> None:
        """
        Closes the connection to the AWS (S3) account if it's currently open.
        """
        self.is_connected = False
        return

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the S3 bucket.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)
        need_to_close = self.is_connected is False

        try:
            connection = self.connect()
            connection.head_bucket(Bucket=self.connection_data['bucket'])
            response.success = True
        except ClientError as e:
            logger.error(f'Error connecting to AWS with the given credentials, {e}!')
            response.error_message = str(e)

        if response.success and need_to_close:
            self.disconnect()

        elif not response.success and self.is_connected:
            self.is_connected = False

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a SQL query on the specified table (object) in the S3 bucket.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        need_to_close = self.is_connected is False

        connection = self.connect()

        # Replace the underscore with a period to get the actual object name.
        key = self.table_name.replace('_', '.')

        # Validate the key extension and set the input serialization accordingly.
        if key.endswith('.csv') or key.endswith('.tsv'):
            input_serialization = {
                'CSV': {
                    'FileHeaderInfo': 'USE'
                }
            }

            if key.endswith('.tsv'):
                input_serialization['CSV']['FieldDelimiter'] = '\t'
        elif key.endswith('.json'):
            input_serialization = {
                'JSON': {
                    'Type': 'Document'
                }
            }
        elif key.endswith('.parquet'):
            input_serialization = {'Parquet': {}}
        else:
            raise ValueError('The Key should have one of the following extensions: .csv, .json, .parquet')

        try:
            result = connection.select_object_content(
                Bucket=self.connection_data['bucket'],
                Key=key,
                ExpressionType='SQL',
                Expression=query,
                InputSerialization=input_serialization,
                OutputSerialization={"JSON": {}}
            )

            if key.endswith('.csv') or key.endswith('.tsv') or key.endswith('.parquet'):
                df = self._parse_json_response_for_csv_and_parquet_input(result)
            elif key.endswith('.json'):
                df = self._parse_response_for_json_input(result)

            response = Response(
                RESPONSE_TYPE.TABLE,
                data_frame=df
            )
        except ClientError as e:
            logger.error(f'Error running query: {query} on {self.table_name} in {self.connection_data["bucket"]}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )
        except Exception as e:
            logger.error(f'An unexpected error occurred while parsing the response: {e}!')
            response = Response(
                RESPONSE_TYPE.ERROR,
                error_message=str(e)
            )

        if need_to_close is True:
            self.disconnect()

        return response
    
    def _parse_json_response_for_csv_and_parquet_input(self, response: Dict) -> pd.DataFrame:
        """
        Parse the JSON response from the select_object_content method.

        Args:
            response (Dict): JSON response from the select_object_content method

        Returns:
            pd.DataFrame: DataFrame containing the parsed response
        """
        all_records = []
        for event in response['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                for record in records.strip().split('\n'):
                    if record:
                        all_records.append(json.loads(record))

        return pd.DataFrame(all_records)
    
    def _parse_response_for_json_input(self, response: Dict) -> pd.DataFrame:
        """
        Parse the JSON response from the select_object_content method for JSON input serialization.

        Args:
            response (Dict): JSON response from the select_object_content method

        Returns:
            pd.DataFrame: DataFrame containing the parsed response
        """
        all_records = []
        for event in response['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')
                record = records.strip()
                if record:
                    json_record = json.loads(record)
                    parsed_json_record = {key: list(value.values()) for key, value in json_record.items()}
                    all_records.append(parsed_json_record)

        df = pd.DataFrame()
        for record in all_records:
            temp_df = pd.DataFrame(record)
            df = pd.concat([df, temp_df], ignore_index=True)

        return df

    def query(self, query: ASTNode) -> Response:
        """
        Executes a SQL query represented by an ASTNode and retrieves the data.

        Args:
            query (ASTNode): An ASTNode representing the SQL query to be executed.

        Returns:
            Response: The response from the `native_query` method, containing the result of the SQL query execution.
        """
        if not isinstance(query, Select):
            raise ValueError('Only SELECT queries are supported.')
        
        # Set the table name by getting the key (file) from the FROM clause of the query.
        # This will be passed as the Key parameter to the select_object_content method.
        from_table = query.from_table
        self.table_name = from_table.parts[0]

        # Replace the value of the FROM clause with 'S3Object'.
        # This is what the select_object_content method expects for all queries.
        query.from_table = Identifier(
            parts=['S3Object'],
            alias=from_table.alias
        )

        return self.native_query(query.to_string())

    def get_tables(self) -> Response:
        """
        Retrieves a list of tables (objects) in the S3 bucket.
        Each object is considered a table. Only CSV, JSON, and Parquet files are supported.
        The period in the object name is replaced with an underscore to allow them to be used as table names in SQL queries.

        Returns:
            Response: A response object containing the list of tables and views, formatted as per the `Response` class.
        """
        connection = self.connect()
        objects = connection.list_objects(Bucket=self.connection_data["bucket"])['Contents']

        # Get only CSV, JSON, and Parquet files.
        # Only these formats are supported select_object_content.
        # Replace the period with an underscore to allow them to be used as table names.
        supported_objects = [obj['Key'].replace('.', '_') for obj in objects if obj['Key'].split('.')[-1] in ['csv', 'json', 'parquet']]

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                supported_objects,
                columns=['table_name']
            )
        )

        return response

    def get_columns(self, table_name: Text) -> Response:
        """
        Retrieves column details for a specified table (object) in the S3 bucket.

        Args:
            table_name (Text): The name of the table for which to retrieve column information.

        Returns:
            Response: A response object containing the column details, formatted as per the `Response` class.
        Raises:
            ValueError: If the 'table_name' is not a valid string.
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError("Invalid table name provided.")

        query = f"SELECT * FROM {table_name} LIMIT 5"
        result = self.native_query(query)

        response = Response(
            RESPONSE_TYPE.TABLE,
            data_frame=pd.DataFrame(
                {
                    'column_name': result.data_frame.columns,
                    'data_type': result.data_frame.dtypes
                }
            )
        )

        return response

import io
import pandas as pd
from typing import List

from mindsdb_sql.parser.ast import Select, Identifier

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.integrations.utilities.sql_utils import FilterOperator, FilterCondition, SortColumn


class S3BucketsTable(APIResource):
    """
    The API table abstraction for S3 buckets.
    """

    def list(self, conditions: List[FilterCondition] = None, limit: int = None, sort: List[SortColumn] = None, targets: List[str] = None):
        """
        Lists the S3 buckets based on the specified conditions when running SELECT statements.

        Args:
            conditions (List[FilterCondition]): The list of filter conditions.
            limit (int): The limit of the number of records to return.
            sort (List[SortColumn]): The list of sort columns.
            targets (List[str]): The list of targets.

        Returns:
            List: The list of S3 buckets based on the specified conditions.
        """
        connection = self.handler.connect()
        buckets = connection.list_buckets()

        return pd.DataFrame(buckets['Buckets'] if 'Buckets' in buckets else [])

    def get_columns(self) -> List:
        """
        Gets the list of columns for the S3 buckets table abstraction.

        Returns:
            List: The list of columns for the S3 buckets table.        
        """
        return ['Name', 'CreationDate']


class S3ObjectsTable(APIResource):
    """
    The API table abstraction for S3 objects.
    """

    def list(self, conditions: List[FilterCondition] = None, limit: int = None, sort: List[SortColumn] = None, targets: List[str] = None):
        """
        Lists the S3 objects based on the specified conditions when running SELECT statements.

        Args:
            conditions (List[FilterCondition]): The list of filter conditions.
            limit (int): The limit of the number of records to return.
            sort (List[SortColumn]): The list of sort columns.
            targets (List[str]): The list of targets.

        Returns:
            List: The list of S3 objects based on the specified conditions.
        """
        connection = self.handler.connect()

        # Apply the WHERE clause.
        # Extract the Bucket parameter.
        if not conditions:
            raise ValueError('A Bucket name should be provided in the WHERE clause.')
        else:
            filters = {}
            for condition in conditions:
                if condition.column == 'Bucket':
                    if condition.op != FilterOperator.EQUAL:
                        raise ValueError("Only the '=' operator is supported for the Bucket column.")
                    
                    filters['Bucket'] = condition.value
                    condition.applied = True
                    
        # Apply the LIMIT clause.
        if limit:
            filters['MaxKeys'] = limit

        objects = connection.list_objects_v2(**filters)

        # Convert the data to a DataFrame and add the Bucket column.
        buckets_df = pd.DataFrame(objects['Contents'] if 'Contents' in objects else [])
        buckets_df['Bucket'] = filters['Bucket']

        return pd.DataFrame(objects['Contents'] if 'Contents' in objects else [])

    def get_columns(self) -> List:
        """
        Gets the list of columns for the S3 objects table abstraction.

        Returns:
            List: The list of columns for the S3 objects table.
        """
        return ['Key', 'LastModified', 'ETag', 'Size', 'StorageClass', 'Bucket']
    

class S3ObjectContentsTable(APIResource):
    """
    The API table abstraction for S3 object contents.
    """
    
    def select(self, query: Select) -> pd.DataFrame:
        """
        Runs SELECT statements against S3 objects.

        Args:
            conditions (List[FilterCondition]): The list of filter conditions.
            limit (int): The limit of the number of records to return.
            sort (List[SortColumn]): The list of sort columns.
            targets (List[str]): The list of targets.

        Returns:
            List: The list of S3 object contents based on the specified conditions.
        """
        connection = self.handler.connect()

        # Extract the Bucket and Key parameters.
        bucket = None
        key = None

        conditions = [
            FilterCondition(i[1], FilterOperator(i[0].upper()), i[2])
            for i in extract_comparison_conditions(query.where)
        ]

        for condition in conditions:
            if condition.column == 'Bucket':
                if condition.op != FilterOperator.EQUAL:
                    raise ValueError("Only the '=' operator is supported for the Bucket column.")
                
                bucket = condition.value

            elif condition.column == 'Key':
                if condition.op != FilterOperator.EQUAL:
                    raise ValueError("Only the '=' operator is supported for the Key column.")
                
                key = condition.value

        if not bucket or not key:
            raise ValueError('Both Bucket and Key names should be provided in the WHERE clause.')
        
        # Validate the Key extension and set the input serialization.
        if key.endswith('.csv'):
            input_serialization = {'CSV': {}}
        elif key.endswith('.json'):
            input_serialization = {'JSON': {}}
        elif key.endswith('.parquet'):
            input_serialization = {'Parquet': {}}
        else:
            raise ValueError('The Key should have one of the following extensions: .csv, .json, .parquet')
        
        # Format the query to pass to the S3 select_object_content method.
        # Remove the Bucket and Key WHERE conditions from the query.
        query.where = [condition for condition in query.where.args if condition.args[0].parts[-1] not in ['Bucket', 'Key']]
        if not query.where:
            query.where = None
        # Update the table of the query to 'S3Object'.
        query.from_table = Identifier('S3Object')
        
        # Run the SELECT statement.
        result = connection.select_object_content(
            Bucket=bucket,
            Key=key,
            ExpressionType='SQL',
            Expression=query.to_string(),
            InputSerialization=input_serialization,
            OutputSerialization={"CSV": {}}
        )

        # Extract the records from the result.
        records = []
        for event in result['Payload']:
            if 'Records' in event:
                records.append(event['Records']['Payload'])

        file_str = ''.join(r.decode('utf-8') for r in records)

        object_contents_df = pd.read_csv(io.StringIO(file_str))

        return object_contents_df

    def get_columns(self) -> List:
        """
        Gets the list of columns for the S3 object contents table abstraction.

        Returns:
            List: The list of columns for the S3 object contents table.
        """
        pass
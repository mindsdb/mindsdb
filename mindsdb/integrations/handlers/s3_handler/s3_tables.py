import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APIResource
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
        # Extract parameters that are supported by the list_objects_v2 method.
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
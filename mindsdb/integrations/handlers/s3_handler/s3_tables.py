from typing import List
from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn


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
        pass

    def get_columns(self) -> List:
        """
        Gets the list of columns for the S3 buckets table abstraction.

        Returns:
            List: The list of columns for the S3 buckets table.        
        """
        pass


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
        pass

    def get_columns(self) -> List:
        """
        Gets the list of columns for the S3 objects table abstraction.

        Returns:
            List: The list of columns for the S3 objects table.
        """
        pass
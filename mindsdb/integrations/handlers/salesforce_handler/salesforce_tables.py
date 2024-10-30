from typing import Dict, List, Text

from mindsdb_sql.parser.ast import Select, Star, Identifier
import pandas as pd

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def create_table_class(table_name: Text, resource_name: Text) -> APIResource:
    """
    Creates a table class for the given Salesforce resource.
    """

    class AnyTable(APIResource):
        """
        This is the table abstraction for any resource of the Salesforce API.
        """

        def select(self, query: Select) -> pd.DataFrame:
            """
            Executes a SELECT SQL query represented by an ASTNode object on the Salesforce resource and retrieves the data (if any).

            Args:
                query (ASTNode): An ASTNode object representing the SQL query to be executed.

            Returns:
                pd.DataFrame: A DataFrame containing the data retrieved from the Salesforce resource.
            """
            query.from_table = table_name

            # SOQL does not support * in SELECT queries. Replace * with column names.
            if isinstance(query.targets[0], Star):
                query.targets = [Identifier(column) for column in self.get_columns()]

            # SOQL does not support column aliases. Remove column aliases.
            column_aliases = {}
            for column in query.targets:
                if column.alias is not None:
                    column_aliases[column.parts[-1]] = column.alias.parts[-1]
                    column.alias = None

            client = self.handler.connect()

            query_str = query.to_string()

            # SOQL does not support backticks. Remove backticks.
            query_str = query_str.replace("`", "")
            results = client.sobjects.query(query_str)

            for result in results:
                del result['attributes']

            df = pd.DataFrame(results)
            df.rename(columns=column_aliases, inplace=True)

            return df

        def add(self, item: Dict) -> None:
            """
            Adds a new item to the Salesforce resource.

            Args:
                contact (Dict): The data to be inserted into the Salesforce resource.
            """
            client = self.handler.connect()
            getattr(client.sobjects, resource_name).insert(item)

        def modify(self, conditions: List[FilterCondition], values: Dict) -> None:
            """
            Modifies items in the Salesforce resource based on the specified conditions.

            Args:
                conditions (List[FilterCondition]): The conditions based on which the items are to be modified.
                values (Dict): The values to be updated in the items.
            """
            client = self.handler.connect()

            ids = self._validate_conditions(conditions)

            for id in ids:
                getattr(client.sobjects, resource_name).update(id, values)

        def remove(self, conditions: List[FilterCondition]) -> None:
            """
            Removes items from the Salesforce resource based on the specified conditions.

            Args:
                conditions (List[FilterCondition]): The conditions based on which the items are to be removed.
            """
            client = self.handler.connect()

            ids = self._validate_conditions(conditions)

            for id in ids:
                getattr(client.sobjects, resource_name).delete(id)

        def _validate_conditions(self, conditions: List[FilterCondition]) -> None:
            """
            Validates the conditions used for filtering items in the Salesforce resource.

            Args:
                conditions (List[FilterCondition]): The conditions to be validated.
            """
            # Salesforce API does not support filtering items based on attributes other than 'Id'. Raise an error if any other column is used.
            if len(conditions) != 1 or conditions[0].column != 'Id':
                raise ValueError("Only the 'Id' column can be used to filter items.")

            # Only the 'equals' and 'in' operators can be used on the 'Id' column for deletion. Raise an error if any other operator is used.
            if conditions[0].op not in [FilterOperator.EQUAL, FilterOperator.IN]:
                raise ValueError("Only the 'equals' and 'in' operators can be used on the 'Id' column.")

            return conditions[0].value if isinstance(conditions[0].value, list) else [conditions[0].value]

        def get_columns(self) -> List[Text]:
            """
            Retrieves the attributes (columns) of the Salesforce resource.

            Returns:
                List[Text]: A list of Attributes (columns) of the Salesforce resource.
            """
            client = self.handler.connect()
            return [field['name'] for field in getattr(client.sobjects, resource_name).describe()['fields']]

    return AnyTable

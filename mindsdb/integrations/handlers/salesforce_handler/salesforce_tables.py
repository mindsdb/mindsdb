from typing import Dict, List, Text

from mindsdb_sql_parser.ast import Select, Star, Identifier
import pandas as pd
from salesforce_api.exceptions import RestRequestCouldNotBeUnderstoodError

from mindsdb.integrations.libs.api_handler import MetaAPIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator
from mindsdb.utilities import log


logger = log.getLogger(__name__)


def create_table_class(resource_name: Text) -> MetaAPIResource:
    """
    Creates a table class for the given Salesforce resource.
    """

    class AnyTable(MetaAPIResource):
        """
        This is the table abstraction for any resource of the Salesforce API.
        """

        def __init__(self, *args, table_name=None, **kwargs):
            """
            Initializes the AnyTable class.

            Args:
                *args: Variable length argument list.
                table_name (str): The name of the table that represents the Salesforce resource.
                **kwargs: Arbitrary keyword arguments.
            """
            super().__init__(*args, table_name=table_name, **kwargs)
            self.resource_metadata = None

        def select(self, query: Select) -> pd.DataFrame:
            """
            Executes a SELECT SQL query represented by an ASTNode object on the Salesforce resource and retrieves the data (if any).

            Args:
                query (ASTNode): An ASTNode object representing the SQL query to be executed.

            Returns:
                pd.DataFrame: A DataFrame containing the data retrieved from the Salesforce resource.
            """
            query.from_table = resource_name

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
                del result["attributes"]

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
            if len(conditions) != 1 or conditions[0].column != "Id":
                raise ValueError("Only the 'Id' column can be used to filter items.")

            # Only the 'equals' and 'in' operators can be used on the 'Id' column for deletion. Raise an error if any other operator is used.
            if conditions[0].op not in [FilterOperator.EQUAL, FilterOperator.IN]:
                raise ValueError("Only the 'equals' and 'in' operators can be used on the 'Id' column.")

            return conditions[0].value if isinstance(conditions[0].value, list) else [conditions[0].value]

        def _get_resource_metadata(self) -> Dict:
            """
            Retrieves metadata about the Salesforce resource.

            Returns:
                Dict: A dictionary containing metadata about the Salesforce resource.
            """
            if self.resource_metadata:
                return self.resource_metadata

            client = self.handler.connect()
            return getattr(client.sobjects, resource_name).describe()

        def get_columns(self) -> List[Text]:
            """
            Retrieves the attributes (columns) of the Salesforce resource.

            Returns:
                List[Text]: A list of Attributes (columns) of the Salesforce resource.
            """
            return [field["name"] for field in self._get_resource_metadata()["fields"]]

        def meta_get_tables(self, table_name: str, main_metadata: Dict) -> Dict:
            """
            Retrieves table metadata for the Salesforce resource.

            Args:
                table_name (str): The name given to the table that represents the Salesforce resource.
                main_metadata (Dict): The main metadata dictionary containing information about all Salesforce resources.

            Returns:
                Dict: A dictionary containing table metadata for the Salesforce resource.
            """
            client = self.handler.connect()

            try:
                resource_metadata = next(
                    (resource for resource in main_metadata if resource["name"].lower() == resource_name),
                )
            except Exception as e:
                logger.warning(f"Failed to get resource metadata for {resource_name}: {e}")
                return {
                    "table_name": table_name,
                    "table_type": "BASE TABLE",
                    "table_description": "",
                    "row_count": None,
                }
            # Get row count if Id column is aggregatable.
            row_count = None
            # if next(field for field in resource_metadata['fields'] if field['name'] == 'Id').get('aggregatable', False):
            try:
                row_count = client.sobjects.query(f"SELECT COUNT(Id) FROM {resource_name}")[0]["expr0"]
            except RestRequestCouldNotBeUnderstoodError as request_error:
                logger.warning(f"Failed to get row count for {resource_name}: {request_error}")

            return {
                "table_name": table_name,
                "table_type": "BASE TABLE",
                "table_description": resource_metadata.get("label", ""),
                "row_count": row_count,
            }

        def meta_get_columns(self, table_name: str) -> List[Dict]:
            """
            Retrieves column metadata for the Salesforce resource.

            Args:
                table_name (str): The name given to the table that represents the Salesforce resource.

            Returns:
                List[Dict]: A list of dictionaries containing column metadata for the Salesforce resource.
            """
            resource_metadata = self._get_resource_metadata()

            column_metadata = []
            for field in resource_metadata["fields"]:
                column_metadata.append(
                    {
                        "table_name": table_name,
                        "column_name": field["name"],
                        "data_type": field["type"],
                        "is_nullable": field.get("nillable", False),
                        "default_value": field.get("defaultValue", ""),
                        "description": field.get("inlineHelpText", ""),
                    }
                )

            return column_metadata

        def meta_get_primary_keys(self, table_name: str) -> List[Dict]:
            """
            Retrieves the primary keys for the Salesforce resource.

            Args:
                table_name (str): The name given to the table that represents the Salesforce resource.

            Returns:
                List[Dict]: A list of dictionaries containing primary key metadata for the Salesforce resource.
            """
            return [
                {
                    "table_name": table_name,
                    "column_name": "Id",
                }
            ]

        def meta_get_foreign_keys(self, table_name: str, all_tables: List[str]) -> List[Dict]:
            """
            Retrieves the foreign keys for the Salesforce resource.

            Args:
                table_name (str): The name given to the table that represents the Salesforce resource.
                all_tables (List[str]): A list of all table names in the Salesforce database.

            Returns:
                List[Dict]: A list of dictionaries containing foreign key metadata for the Salesforce resource.
            """
            resource_metadata = self._get_resource_metadata()

            foreign_key_metadata = []
            for child_relationship in resource_metadata.get("childRelationships", []):
                # Skip if the child relationship is not one of the supported tables.
                child_table_name = child_relationship["childSObject"]
                if child_table_name not in all_tables:
                    continue

                foreign_key_metadata.append(
                    {
                        "parent_table_name": table_name,
                        "parent_column_name": "Id",
                        "child_table_name": child_table_name,
                        "child_column_name": child_relationship["field"],
                    }
                )

            return foreign_key_metadata

    return AnyTable

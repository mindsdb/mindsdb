from typing import Any, Dict, List, Optional, Text

import pandas as pd
import salesforce_api
from salesforce_api.exceptions import AuthenticationError, RestRequestCouldNotBeUnderstoodError

from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.libs.response import (
    HandlerResponse as Response,
    HandlerStatusResponse as StatusResponse,
    RESPONSE_TYPE,
)
from mindsdb.integrations.handlers.salesforce_handler.salesforce_tables import create_table_class
from mindsdb.utilities import log


logger = log.getLogger(__name__)


class SalesforceHandler(MetaAPIHandler):
    """
    This handler handles the connection and execution of SQL statements on Salesforce.
    """

    name = "salesforce"

    def __init__(self, name: Text, connection_data: Dict, **kwargs: Any) -> None:
        """
        Initializes the handler.

        Args:
            name (Text): The name of the handler instance.
            connection_data (Dict): The connection data required to connect to the Salesforce API.
            kwargs: Arbitrary keyword arguments.
        """
        super().__init__(name)
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection = None
        self.is_connected = False
        self.thread_safe = True
        self.resource_names = []

    def connect(self) -> salesforce_api.client.Client:
        """
        Establishes a connection to the Salesforce API.

        Raises:
            ValueError: If the required connection parameters are not provided.
            AuthenticationError: If an authentication error occurs while connecting to the Salesforce API.

        Returns:
            salesforce_api.client.Client: A connection object to the Salesforce API.
        """
        if self.is_connected is True:
            return self.connection

        # Mandatory connection parameters.
        if not all(key in self.connection_data for key in ["username", "password", "client_id", "client_secret"]):
            raise ValueError("Required parameters (username, password, client_id, client_secret) must be provided.")

        try:
            self.connection = salesforce_api.Salesforce(
                username=self.connection_data["username"],
                password=self.connection_data["password"],
                client_id=self.connection_data["client_id"],
                client_secret=self.connection_data["client_secret"],
                is_sandbox=self.connection_data.get("is_sandbox", False),
            )
            self.is_connected = True

            resource_tables = self._get_resource_names()
            for resource_name in resource_tables:
                table_class = create_table_class(resource_name)
                self._register_table(resource_name, table_class(self))

            return self.connection
        except AuthenticationError as auth_error:
            logger.error(f"Authentication error connecting to Salesforce, {auth_error}!")
            raise
        except Exception as unknown_error:
            logger.error(f"Unknwn error connecting to Salesforce, {unknown_error}!")
            raise

    def check_connection(self) -> StatusResponse:
        """
        Checks the status of the connection to the Salesforce API.

        Returns:
            StatusResponse: An object containing the success status and an error message if an error occurs.
        """
        response = StatusResponse(False)

        try:
            self.connect()
            response.success = True
        except (AuthenticationError, ValueError) as known_error:
            logger.error(f"Connection check to Salesforce failed, {known_error}!")
            response.error_message = str(known_error)
        except Exception as unknown_error:
            logger.error(f"Connection check to Salesforce failed due to an unknown error, {unknown_error}!")
            response.error_message = str(unknown_error)

        self.is_connected = response.success

        return response

    def native_query(self, query: Text) -> Response:
        """
        Executes a native SOQL query on Salesforce and returns the result.

        Args:
            query (Text): The SQL query to be executed.

        Returns:
            Response: A response object containing the result of the query or an error message.
        """
        connection = self.connect()

        try:
            results = connection.sobjects.query(query)

            parsed_results = []
            for result in results:
                del result["attributes"]

                # Check if the result contains any of the other Salesforce resources.
                if any(key in self.resource_names for key in result.keys()):
                    # Parse the result to extract the nested resources.
                    parsed_result = {}
                    for key, value in result.items():
                        if key in self.resource_names:
                            del value["attributes"]
                            parsed_result.update(
                                {f"{key}_{sub_key}": sub_value for sub_key, sub_value in value.items()}
                            )

                        else:
                            parsed_result[key] = value

                    parsed_results.append(parsed_result)

                else:
                    parsed_results.append(result)

            response = Response(RESPONSE_TYPE.TABLE, pd.DataFrame(parsed_results))
        except RestRequestCouldNotBeUnderstoodError as rest_error:
            logger.error(f"Error running query: {query} on Salesforce, {rest_error}!")
            response = Response(RESPONSE_TYPE.ERROR, error_code=0, error_message=str(rest_error))
        except Exception as unknown_error:
            logger.error(f"Error running query: {query} on Salesforce, {unknown_error}!")
            response = Response(RESPONSE_TYPE.ERROR, error_code=0, error_message=str(unknown_error))

        return response

    def _get_resource_names(self) -> List[str]:
        """
        Retrieves the names of the Salesforce resources, with more aggressive filtering to remove tables.
        Returns:
            List[str]: A list of filtered resource names.
        """
        if not self.resource_names:
            all_resources = [
                resource["name"]
                for resource in self.connection.sobjects.describe()["sobjects"]
                if resource.get("queryable", False)
            ]

            # Define patterns for tables to be filtered out.
            # Expanded suffixes and prefixes and exact matches
            ignore_suffixes = ("Share", "History", "Feed", "ChangeEvent", "Tag", "Permission", "Setup", "Consent")
            ignore_prefixes = (
                "Apex",
                "CommPlatform",
                "Lightning",
                "Flow",
                "Transaction",
                "AI",
                "Aura",
                "ContentWorkspace",
                "Collaboration",
                "Datacloud",
            )
            ignore_exact = {
                "EntityDefinition",
                "FieldDefinition",
                "RecordType",
                "CaseStatus",
                "UserRole",
                "UserLicense",
                "UserPermissionAccess",
                "UserRecordAccess",
                "Folder",
                "Group",
                "Note",
                "ProcessDefinition",
                "ProcessInstance",
                "ContentFolder",
                "ContentDocumentSubscription",
                "DashboardComponent",
                "Report",
                "Dashboard",
                "Topic",
                "TopicAssignment",
                "Period",
                "Partner",
                "PackageLicense",
                "ColorDefinition",
                "DataUsePurpose",
                "DataUseLegalBasis",
            }

            ignore_substrings = (
                "CleanInfo",
                "Template",
                "Rule",
                "Definition",
                "Status",
                "Policy",
                "Setting",
                "Access",
                "Config",
                "Subscription",
                "DataType",
                "MilestoneType",
                "Entitlement",
                "Auth",
            )

            filtered = []
            for r in all_resources:
                if (
                    not r.endswith(ignore_suffixes)
                    and not r.startswith(ignore_prefixes)
                    and not any(sub in r for sub in ignore_substrings)
                    and r not in ignore_exact
                ):
                    filtered.append(r)

            self.resource_names = [r for r in filtered]
        return self.resource_names

    def meta_get_handler_info(self, **kwargs) -> str:
        """
        Retrieves information about the design and implementation of the API handler.
        This should include, but not be limited to, the following:
        - The type of SQL queries and operations that the handler supports.
        - etc.

        Args:
            kwargs: Additional keyword arguments that may be used in generating the handler information.

        Returns:
            str: A string containing information about the API handler's design and implementation.
        """
        # TODO: Relationships? Aliases?
        return "When filtering on a Date or DateTime field, the value MUST be an unquoted literal in YYYY-MM-DD or YYYY-MM-DDThh:mm:ssZ format. For example, CloseDate >= 2025-05-28 is correct; CloseDate >= '2025-05-28' is incorrect."

    def meta_get_tables(self, table_names: Optional[List[str]] = None) -> Response:
        """
        Retrieves metadata for the specified tables (or all tables if no list is provided).

        Args:
            table_names (List): A list of table names for which to retrieve metadata.

        Returns:
            Response: A response object containing the table metadata.
        """
        connection = self.connect()

        # Retrieve the metadata for all Salesforce resources.
        main_metadata = connection.sobjects.describe()

        if table_names:
            # Filter the metadata for the specified tables.
            main_metadata = [resource for resource in main_metadata["sobjects"] if resource["name"] in table_names]
        else:
            main_metadata = main_metadata["sobjects"]

        return super().meta_get_tables(table_names=table_names, main_metadata=main_metadata)

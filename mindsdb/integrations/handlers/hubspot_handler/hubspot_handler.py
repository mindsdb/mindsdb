from typing import Optional, List, Dict, Any
import pandas as pd
from pandas.api import types as pd_types
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import (
    ContactsTable,
    CompaniesTable,
    DealsTable,
    HUBSPOT_TABLE_COLUMN_DEFINITIONS,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


def _extract_hubspot_error_message(error: Exception) -> str:
    """Extract a user-friendly error message from HubSpot API exceptions.

    Args:
        error (Exception): The exception from HubSpot API

    Returns:
        str: A clear, actionable error message
    """
    error_str = str(error)

    # Check for missing scopes error (403)
    if "403" in error_str and "MISSING_SCOPES" in error_str:
        # Try to extract required scopes from error message
        if "requiredGranularScopes" in error_str:
            import json

            try:
                # Extract JSON from error message
                start = error_str.find('{"status":')
                if start != -1:
                    json_str = error_str[start : error_str.find("}", start) + 1]
                    error_data = json.loads(json_str)

                    if "errors" in error_data and len(error_data["errors"]) > 0:
                        context = error_data["errors"][0].get("context", {})
                        scopes = context.get("requiredGranularScopes", [])

                        if scopes:
                            scopes_list = ", ".join(scopes)
                            return (
                                f"Missing required HubSpot scopes. Your access token needs one or more of these permissions: {scopes_list}. "
                                f"Please update your HubSpot app scopes at https://developers.hubspot.com/ and regenerate your access token."
                            )
            except (json.JSONDecodeError, KeyError, IndexError):
                pass

        return (
            "Missing required HubSpot API permissions (scopes). "
            "Please verify your access token has the necessary scopes: "
            "crm.objects.companies.read, crm.objects.contacts.read, crm.objects.deals.read. "
            "Update scopes at https://developers.hubspot.com/"
        )

    # Check for authentication errors (401)
    if "401" in error_str or "Unauthorized" in error_str:
        return (
            "Invalid or expired HubSpot access token. "
            "Please regenerate your access token at https://developers.hubspot.com/"
        )

    # Check for rate limiting (429)
    if "429" in error_str or "rate limit" in error_str.lower():
        return "HubSpot API rate limit exceeded. Please wait a moment and try again."

    # Generic HubSpot API error
    if "ApiException" in error_str or "hubspot" in error_str.lower():
        return f"HubSpot API error: {error_str[:200]}"

    # Return original error message
    return str(error)


def _map_type(data_type: str) -> MYSQL_DATA_TYPE:
    """Map HubSpot data types to MySQL types.

    Args:
        data_type (str): The HubSpot/SQL data type name

    Returns:
        MYSQL_DATA_TYPE: The corresponding MySQL data type
    """
    if data_type is None:
        return MYSQL_DATA_TYPE.VARCHAR

    data_type_upper = data_type.upper()

    type_map = {
        "VARCHAR": MYSQL_DATA_TYPE.VARCHAR,
        "TEXT": MYSQL_DATA_TYPE.TEXT,
        "INTEGER": MYSQL_DATA_TYPE.INT,
        "INT": MYSQL_DATA_TYPE.INT,
        "BIGINT": MYSQL_DATA_TYPE.BIGINT,
        "DECIMAL": MYSQL_DATA_TYPE.DECIMAL,
        "FLOAT": MYSQL_DATA_TYPE.FLOAT,
        "DOUBLE": MYSQL_DATA_TYPE.DOUBLE,
        "BOOLEAN": MYSQL_DATA_TYPE.BOOL,
        "BOOL": MYSQL_DATA_TYPE.BOOL,
        "DATE": MYSQL_DATA_TYPE.DATE,
        "DATETIME": MYSQL_DATA_TYPE.DATETIME,
        "TIMESTAMP": MYSQL_DATA_TYPE.DATETIME,
        "TIME": MYSQL_DATA_TYPE.TIME,
    }

    return type_map.get(data_type_upper, MYSQL_DATA_TYPE.VARCHAR)


class HubspotHandler(MetaAPIHandler):
    """Hubspot API handler implementation"""

    name = "hubspot"

    def __init__(self, name: str, **kwargs: Any) -> None:
        """
        Initialize the handler.

        Args:
            name (str): name of particular handler instance
            **kwargs: arbitrary keyword arguments including connection_data
        """
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection: Optional[HubSpot] = None
        self.is_connected: bool = False

        # Register tables for data catalog
        companies_data = CompaniesTable(self)
        self._register_table("companies", companies_data)

        contacts_data = ContactsTable(self)
        self._register_table("contacts", contacts_data)

        deals_data = DealsTable(self)
        self._register_table("deals", deals_data)

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client if needed and sets it as the client to use for requests.

        Returns:
            HubSpot: Newly created Hubspot API client, or current client if already set.

        Raises:
            ValueError: If authentication credentials are missing or invalid.
            Exception: If connection to HubSpot API fails.
        """
        if self.is_connected and self.connection is not None:
            return self.connection

        try:
            if "access_token" in self.connection_data:
                access_token = self.connection_data["access_token"]
                if not access_token or not isinstance(access_token, str):
                    raise ValueError("Invalid access_token provided")

                logger.info("Connecting to HubSpot using access token")
                self.connection = HubSpot(access_token=access_token)

            elif "client_id" in self.connection_data and "client_secret" in self.connection_data:
                client_id = self.connection_data["client_id"]
                client_secret = self.connection_data["client_secret"]

                if not client_id or not client_secret:
                    raise ValueError("Invalid OAuth credentials provided")

                logger.info("Connecting to HubSpot using OAuth credentials")

                self.connection = HubSpot(client_id=client_id, client_secret=client_secret)
            else:
                raise ValueError(
                    "Authentication credentials missing. Provide either 'access_token' "
                    "or both 'client_id' and 'client_secret' for OAuth authentication."
                )

            self.is_connected = True
            logger.info("Successfully connected to HubSpot API")
            return self.connection

        except ValueError:
            logger.error("Failed to connect to HubSpot API")
            raise
        except Exception as e:
            logger.error("Failed to connect to HubSpot API")
            raise ValueError(f"Connection to HubSpot failed: {str(e)}")

    def disconnect(self) -> None:
        """Close connection and cleanup resources."""
        self.connection = None
        self.is_connected = False
        logger.info("Disconnected from HubSpot API")

    def check_connection(self) -> StatusResponse:
        """Checks whether the API client is connected to Hubspot.

        Returns:
            StatusResponse: A status response indicating whether the API client is connected to Hubspot.
        """
        response = StatusResponse(False)

        try:
            self.connect()

            if self.connection:
                # Try to access contacts first (most common scope)
                try:
                    list(self.connection.crm.contacts.get_all(limit=1))
                    response.success = True
                    logger.info("HubSpot connection check successful (contacts accessible)")
                except Exception as contacts_error:
                    # If contacts fail, try companies as fallback
                    try:
                        list(self.connection.crm.companies.get_all(limit=1))
                        response.success = True
                        logger.info("HubSpot connection check successful (companies accessible)")
                    except Exception as companies_error:
                        # If both fail, report both errors for better context
                        contacts_msg = _extract_hubspot_error_message(contacts_error)
                        companies_msg = _extract_hubspot_error_message(companies_error)
                        error_msg = f"Cannot access HubSpot data. Contacts error: {contacts_msg}. Companies error: {companies_msg}"
                        logger.error(f"HubSpot connection check failed: {error_msg}")
                        response.error_message = error_msg
                        response.success = False

        except Exception as e:
            error_msg = _extract_hubspot_error_message(e)
            logger.error(f"HubSpot connection check failed: {error_msg}")
            response.error_message = error_msg
            response.success = False

        self.is_connected = response.success
        return response

    def native_query(self, query: Optional[str] = None) -> Response:
        """Receive and process a raw query.

        Args:
            query (str): query in a native format (SQL)

        Returns:
            Response: Response containing query results or error information

        Raises:
            ValueError: If query is None or empty
            Exception: If query parsing or execution fails
        """
        if not query:
            return Response(RESPONSE_TYPE.ERROR, error_message="Query cannot be None or empty")

        try:
            ast = parse_sql(query)
            return self.query(ast)
        except Exception as e:
            logger.error(f"Failed to execute native query: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Query execution failed: {str(e)}")

    def get_tables(self) -> Response:
        """Return list of tables available in the HubSpot integration.

        Returns:
            Response: A response containing table metadata including table names, types,
            estimated row counts, and descriptions.
        """
        try:
            self.connect()

            tables_data = []
            accessible_tables = ["companies", "contacts", "deals"]

            # Check which tables are accessible based on scopes
            for table_name in accessible_tables:
                try:
                    # Try to access each table with a minimal request
                    default_properties = self._tables[table_name].get_columns()
                    getattr(self.connection.crm, table_name).get_all(limit=1, properties=default_properties)

                    # If successful, get full metadata
                    table_info = {
                        "TABLE_SCHEMA": "hubspot",
                        "TABLE_NAME": table_name,
                        "TABLE_TYPE": "BASE TABLE",
                    }
                    tables_data.append(table_info)
                    logger.info(f"Table '{table_name}' is accessible")
                except Exception as access_error:
                    # Table is not accessible (likely missing scope)
                    if "403" in str(access_error) or "MISSING_SCOPES" in str(access_error):
                        error_msg = _extract_hubspot_error_message(access_error)
                        logger.warning(f"Table '{table_name}' is not accessible: {error_msg}")
                    else:
                        logger.warning(f"Could not access table {table_name}: {str(access_error)}")

            if not tables_data:
                error_msg = (
                    "No HubSpot tables are accessible with your current access token. "
                    "Please ensure your token has at least one of these scopes: "
                    "crm.objects.companies.read, crm.objects.contacts.read, or crm.objects.deals.read. "
                    "Update scopes at https://developers.hubspot.com/"
                )
                logger.error(error_msg)
                return Response(RESPONSE_TYPE.ERROR, error_message=error_msg)

            df = pd.DataFrame(tables_data)
            logger.info(
                f"Retrieved metadata for {len(tables_data)} accessible table(s): {', '.join(accessible_tables)}"
            )
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            error_msg = _extract_hubspot_error_message(e)
            logger.error(f"Failed to get tables: {error_msg}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve table list: {error_msg}")

    def get_columns(self, table_name: str) -> Response:
        """Return column information for a specific table in standard information_schema.columns format.

        Args:
            table_name (str): Name of the table to get column information for

        Returns:
            Response: A response containing column metadata in standard information_schema.columns format.
        """
        if table_name not in ["companies", "contacts", "deals"]:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Table '{table_name}' not found. Available tables: companies, contacts, deals",
            )

        try:
            self.connect()

            # Discover columns from HubSpot API
            discovered_columns = self._discover_columns(table_name, sample_size=100)

            # Transform to information_schema.columns format (12 standard fields)
            columns_data = []
            for col in discovered_columns:
                columns_data.append(
                    {
                        "COLUMN_NAME": col["column_name"],
                        "DATA_TYPE": col["data_type"],
                        "ORDINAL_POSITION": col["ordinal_position"],
                        "COLUMN_DEFAULT": None,
                        "IS_NULLABLE": "YES"
                        if col["is_nullable"] is True
                        else ("NO" if col["is_nullable"] is False else None),
                        "CHARACTER_MAXIMUM_LENGTH": None,
                        "CHARACTER_OCTET_LENGTH": None,
                        "NUMERIC_PRECISION": None,
                        "NUMERIC_SCALE": None,
                        "DATETIME_PRECISION": None,
                        "CHARACTER_SET_NAME": None,
                        "COLLATION_NAME": None,
                    }
                )

            df = pd.DataFrame(columns_data)
            logger.info(f"Retrieved {len(columns_data)} columns for table {table_name}")

            result = Response(RESPONSE_TYPE.TABLE, data_frame=df)
            result.to_columns_table_response(map_type_fn=_map_type)
            return result

        except Exception as e:
            error_msg = _extract_hubspot_error_message(e)
            logger.error(f"Failed to get columns for table {table_name}: {error_msg}")

            return Response(
                RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve columns for table '{table_name}': {error_msg}"
            )

    def meta_get_column_statistics(self, table_names: Optional[List[str]] = None) -> Response:
        """Return column statistics for data catalog

        Args:
            table_names (Optional[List[str]]): List of table names to get statistics for,
            or None for all tables

        Returns:
            Response: A response containing column statistics with fields:
                     TABLE_NAME, COLUMN_NAME, MOST_COMMON_VALUES,
                     MOST_COMMON_FREQUENCIES, NULL_PERCENTAGE, MINIMUM_VALUE,
                     MAXIMUM_VALUE, DISTINCT_VALUES_COUNT
        """
        try:
            self.connect()

            all_tables = ["companies", "contacts", "deals"]
            if table_names:
                tables_to_process = [t for t in table_names if t in all_tables]
            else:
                tables_to_process = all_tables

            all_statistics = []

            for table_name in tables_to_process:
                try:
                    # Get sample data for statistics (use larger sample for better accuracy)
                    default_properties = self._tables[table_name].get_columns()
                    sample_data = list(
                        getattr(self.connection.crm, table_name).get_all(limit=1000, properties=default_properties)
                    )

                    if len(sample_data) > 0:
                        sample_size = len(sample_data)
                        logger.info(f"Calculating statistics from {sample_size} records for {table_name}")

                        # Get all unique properties
                        all_properties = set()
                        for item in sample_data:
                            if hasattr(item, "properties") and item.properties:
                                all_properties.update(item.properties.keys())

                        # Calculate statistics for 'id' column
                        id_values = [item.id for item in sample_data]
                        id_stats = self._calculate_column_statistics("id", id_values)
                        all_statistics.append(
                            {
                                "TABLE_NAME": table_name,
                                "COLUMN_NAME": "id",
                                "NULL_PERCENTAGE": (id_stats["null_count"] / sample_size) * 100
                                if sample_size > 0
                                else 0,
                                "DISTINCT_VALUES_COUNT": id_stats["distinct_count"],
                                "MINIMUM_VALUE": None,
                                "MAXIMUM_VALUE": None,
                                "MOST_COMMON_VALUES": None,
                                "MOST_COMMON_FREQUENCIES": None,
                            }
                        )

                        # Calculate statistics for each property column
                        for prop_name in sorted(all_properties):
                            column_name = prop_name
                            if prop_name == "hs_lastmodifieddate":
                                column_name = "lastmodifieddate"

                            # Collect values
                            column_values = []
                            for item in sample_data:
                                if hasattr(item, "properties") and item.properties:
                                    column_values.append(item.properties.get(prop_name))
                                else:
                                    column_values.append(None)

                            stats = self._calculate_column_statistics(prop_name, column_values)

                            # Calculate most common values and their frequencies
                            most_common_values = None
                            most_common_frequencies = None
                            non_null_values = [v for v in column_values if v is not None]
                            if non_null_values:
                                from collections import Counter

                                value_counts = Counter(non_null_values)
                                top_5 = value_counts.most_common(5)
                                if top_5:
                                    most_common_values = [str(v) for v, _ in top_5]
                                    most_common_frequencies = [str(c) for _, c in top_5]

                            all_statistics.append(
                                {
                                    "TABLE_NAME": table_name,
                                    "COLUMN_NAME": column_name,
                                    "NULL_PERCENTAGE": (stats["null_count"] / sample_size) * 100
                                    if sample_size > 0
                                    else 0,
                                    "DISTINCT_VALUES_COUNT": stats["distinct_count"],
                                    "MINIMUM_VALUE": None,
                                    "MAXIMUM_VALUE": None,
                                    "MOST_COMMON_VALUES": most_common_values,
                                    "MOST_COMMON_FREQUENCIES": most_common_frequencies,
                                }
                            )
                            all_statistics = [
                                column for column in all_statistics if column["COLUMN_NAME"] in default_properties
                            ]

                except Exception as e:
                    logger.warning(f"Could not get statistics for table {table_name}: {str(e)}")

            df = pd.DataFrame(all_statistics)
            logger.info(
                f"Retrieved statistics for {len(all_statistics)} columns across {len(tables_to_process)} tables"
            )
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Failed to get column statistics: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve column statistics: {str(e)}")

    def _discover_columns(self, table_name: str, sample_size: int = 100) -> List[Dict[str, Any]]:
        """Discover columns from HubSpot API by sampling data.

        Args:
            table_name (str): Name of the table (companies, contacts, or deals)
            sample_size (int): Number of records to sample for column discovery

        Returns:
            List[Dict[str, Any]]: List of discovered columns with:
                - column_name: Name of the column
                - data_type: Inferred SQL data type
                - is_nullable: Whether the column can contain NULL values
                - ordinal_position: Position in the table
                - description: Human-readable description
                - original_name: Original property name from HubSpot

        Raises:
            Exception: If there's a permissions error accessing the table
        """
        try:
            # Fetch sample data based on table type
            sample_data = None
            if table_name == "companies":
                sample_data = list(self.connection.crm.companies.get_all(limit=sample_size))
            elif table_name == "contacts":
                sample_data = list(self.connection.crm.contacts.get_all(limit=sample_size))
            elif table_name == "deals":
                sample_data = list(self.connection.crm.deals.get_all(limit=sample_size))

            if not sample_data or len(sample_data) == 0:
                logger.warning(f"No data available for {table_name}, using defaults")
                return self._get_default_discovered_columns(table_name)

            logger.info(f"Analyzing {len(sample_data)} records for {table_name} column discovery")

            # Discover all unique properties from the sample
            all_properties = set()
            for item in sample_data:
                if hasattr(item, "properties") and item.properties:
                    all_properties.update(item.properties.keys())

            discovered_columns = []
            ordinal_position = 1

            # Add 'id' column first (primary key, always present)
            discovered_columns.append(
                {
                    "column_name": "id",
                    "data_type": "VARCHAR",
                    "is_nullable": False,
                    "ordinal_position": ordinal_position,
                    "description": "Unique identifier for the record (Primary Key)",
                    "original_name": "id",
                }
            )
            ordinal_position += 1

            # Add columns for each discovered property
            for prop_name in sorted(all_properties):
                # Map property name to column name
                column_name = prop_name
                if prop_name == "hs_lastmodifieddate":
                    column_name = "lastmodifieddate"

                # Collect sample values for type inference and nullability detection
                column_values = []
                for item in sample_data:
                    if hasattr(item, "properties") and item.properties:
                        column_values.append(item.properties.get(prop_name))
                    else:
                        column_values.append(None)

                # Infer data type from samples
                data_type = self._infer_data_type_from_samples(column_values)

                discovered_columns.append(
                    {
                        "column_name": column_name,
                        "data_type": data_type,
                        "is_nullable": None,
                        "ordinal_position": ordinal_position,
                        "description": f"HubSpot property: {prop_name}",
                        "original_name": prop_name,
                    }
                )
                ordinal_position += 1

            logger.info(f"Discovered {len(discovered_columns)} columns for {table_name}")
            return discovered_columns

        except Exception as e:
            if "403" in str(e) or "MISSING_SCOPES" in str(e) or "401" in str(e):
                error_msg = _extract_hubspot_error_message(e)
                logger.error(f"Permission error discovering columns for {table_name}: {error_msg}")
                raise Exception(error_msg) from e

            logger.warning(f"Could not discover columns for {table_name}, using defaults: {str(e)}")
            return self._get_default_discovered_columns(table_name)

    def _get_default_discovered_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get default discovered columns when API data is unavailable.

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict[str, Any]]: List of default discovered columns
        """
        ordinal_position = 1
        base_columns = [
            {
                "column_name": "id",
                "data_type": "VARCHAR",
                "is_nullable": False,
                "ordinal_position": ordinal_position,
                "description": "Unique identifier (Primary Key)",
                "original_name": "id",
            }
        ]
        ordinal_position += 1

        if table_name in HUBSPOT_TABLE_COLUMN_DEFINITIONS:
            for col_name, data_type, description in HUBSPOT_TABLE_COLUMN_DEFINITIONS[table_name]:
                base_columns.append(
                    {
                        "column_name": col_name,
                        "data_type": data_type,
                        "is_nullable": True,
                        "ordinal_position": ordinal_position,
                        "description": description,
                        "original_name": col_name,
                    }
                )
                ordinal_position += 1

        return base_columns

    def _get_default_meta_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get default column metadata for data catalog when data is unavailable

        Args:
            table_name (str): Name of the table

        Returns:
            List[Dict[str, Any]]: List of column metadata dictionaries
        """
        base_columns = [
            {
                "TABLE_NAME": table_name,
                "COLUMN_NAME": "id",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Unique identifier (Primary Key)",
                "IS_NULLABLE": False,
                "COLUMN_DEFAULT": None,
            }
        ]

        if table_name in HUBSPOT_TABLE_COLUMN_DEFINITIONS:
            for col_name, data_type, description in HUBSPOT_TABLE_COLUMN_DEFINITIONS[table_name]:
                base_columns.append(
                    {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": col_name,
                        "DATA_TYPE": data_type,
                        "COLUMN_DESCRIPTION": description,
                        "IS_NULLABLE": True,
                        "COLUMN_DEFAULT": None,
                    }
                )

        return base_columns

    def _get_table_description(self, table_name: str) -> str:
        """Get description for a table."""
        descriptions = {
            "companies": "HubSpot companies data including name, industry, location and other company properties",
            "contacts": "HubSpot contacts data including email, name, phone and other contact properties",
            "deals": "HubSpot deals data including deal name, amount, stage and other deal properties",
        }
        return descriptions.get(table_name, f"HubSpot {table_name} data")

    def _estimate_table_rows(self, table_name: str) -> Optional[int]:
        """Get actual count of rows in a table using HubSpot Search API

        Args:
            table_name (str): Name of the table (companies, contacts, or deals)

        Returns:
            Optional[int]: Total number of records, or None if count cannot be determined
        """
        try:
            if table_name == "companies":
                result = self.connection.crm.companies.search_api.do_search(public_object_search_request={"limit": 1})
                return result.total if hasattr(result, "total") else None
            elif table_name == "contacts":
                result = self.connection.crm.contacts.search_api.do_search(public_object_search_request={"limit": 1})
                return result.total if hasattr(result, "total") else None
            elif table_name == "deals":
                result = self.connection.crm.deals.search_api.do_search(public_object_search_request={"limit": 1})
                return result.total if hasattr(result, "total") else None
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name} using search API: {str(e)}")
        return None

    def _calculate_column_statistics(self, column_name: str, values: List[Any]) -> Dict[str, Any]:
        """Calculate comprehensive statistics for a column."""
        total_count = len(values)
        non_null_values = [v for v in values if v is not None]
        null_count = total_count - len(non_null_values)

        stats = {
            "null_count": null_count,
            "distinct_count": len(set(str(v) for v in non_null_values)) if non_null_values else 0,
            "min_value": None,
            "max_value": None,
            "average_value": None,
        }

        if non_null_values:
            # Try to calculate numeric average for numeric columns using pandas
            try:
                s = pd.Series(non_null_values)
                if pd_types.is_numeric_dtype(s):
                    avg = s.mean()
                    stats["average_value"] = round(avg, 2)
            except (ValueError, TypeError):
                # Not numeric data, average stays None
                pass

        return stats

    def _infer_data_type_from_samples(self, values: List[Any]) -> str:
        """Infer data type from multiple sample values for better accuracy."""
        non_null_values = [v for v in values if v is not None]

        if not non_null_values:
            return "VARCHAR"

        # Analyze types across all samples
        type_counts = {}
        for value in non_null_values[:100]:  # Sample first 100 for performance
            inferred_type = self._infer_data_type(value)
            type_counts[inferred_type] = type_counts.get(inferred_type, 0) + 1

        # Return the most common type
        if type_counts:
            return max(type_counts.items(), key=lambda x: x[1])[0]

        return "VARCHAR"

    def _infer_data_type(self, value: Any) -> str:
        """Infer SQL data type from Python value."""
        if value is None:
            return "VARCHAR"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "DECIMAL"
        elif isinstance(value, str):
            if "T" in value and ("Z" in value or "+" in value):
                return "TIMESTAMP"
            return "VARCHAR"
        else:
            return "VARCHAR"

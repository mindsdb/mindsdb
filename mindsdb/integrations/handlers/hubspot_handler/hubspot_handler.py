from typing import Optional, List, Dict, Any
import pandas as pd
from pandas.api import types as pd_types
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import (
    ContactsTable,
    CompaniesTable,
    DealsTable,
    TicketsTable,
    TasksTable,
    CallsTable,
    EmailsTable,
    MeetingsTable,
    NotesTable,
    to_hubspot_property,
    to_internal_property,
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
    """Extract a user-friendly error message from HubSpot API exceptions."""
    error_str = str(error)

    if "403" in error_str and "MISSING_SCOPES" in error_str:
        if "requiredGranularScopes" in error_str:
            import json

            try:
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
            "Please verify your access token has the necessary scopes. "
            "Update scopes at https://developers.hubspot.com/"
        )

    if "401" in error_str or "Unauthorized" in error_str:
        return "Invalid or expired HubSpot access token. Please regenerate your access token at https://developers.hubspot.com/"

    if "429" in error_str or "rate limit" in error_str.lower():
        return "HubSpot API rate limit exceeded. Please wait a moment and try again."

    if "ApiException" in error_str or "hubspot" in error_str.lower():
        return f"HubSpot API error: {error_str[:200]}"

    return str(error)


def _map_type(data_type: str) -> MYSQL_DATA_TYPE:
    """Map HubSpot data types to MySQL types."""
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
        """Initialize the handler."""
        super().__init__(name)

        connection_data = kwargs.get("connection_data", {})
        self.connection_data = connection_data
        self.kwargs = kwargs

        self.connection: Optional[HubSpot] = None
        self.is_connected: bool = False

        # Register core CRM tables
        self._register_table("companies", CompaniesTable(self))
        self._register_table("contacts", ContactsTable(self))
        self._register_table("deals", DealsTable(self))
        self._register_table("tickets", TicketsTable(self))

        # Register engagement/activity tables
        self._register_table("tasks", TasksTable(self))
        self._register_table("calls", CallsTable(self))
        self._register_table("emails", EmailsTable(self))
        self._register_table("meetings", MeetingsTable(self))
        self._register_table("notes", NotesTable(self))

    def connect(self) -> HubSpot:
        """Creates a new Hubspot API client if needed."""
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
        """Checks whether the API client is connected to Hubspot."""
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
                    try:
                        list(self.connection.crm.companies.get_all(limit=1))
                        response.success = True
                        logger.info("HubSpot connection check successful (companies accessible)")
                    except Exception as companies_error:
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
        """Receive and process a raw query."""
        if not query:
            return Response(RESPONSE_TYPE.ERROR, error_message="Query cannot be None or empty")

        try:
            ast = parse_sql(query)
            return self.query(ast)
        except Exception as e:
            logger.error(f"Failed to execute native query: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Query execution failed: {str(e)}")

    def get_tables(self) -> Response:
        """Return list of tables available in the HubSpot integration."""
        try:
            self.connect()

            tables_data = []
            all_tables = ["companies", "contacts", "deals", "tickets", "tasks", "calls", "emails", "meetings", "notes"]
            for table_name in all_tables:
                try:
                    # Try to access each table with a minimal request
                    default_properties = self._tables[table_name].get_columns()
                    hubspot_properties = [
                        to_hubspot_property(col)
                        for col in default_properties
                        if to_hubspot_property(col) != "hs_object_id"
                    ]

                    # Different API paths for different object types
                    if table_name in ["companies", "contacts", "deals", "tickets"]:
                        getattr(self.connection.crm, table_name).get_all(limit=1, properties=hubspot_properties)
                    else:
                        # Engagement objects use crm.objects; fetch a single page to validate access.
                        self.connection.crm.objects.basic_api.get_page(
                            table_name, limit=1, properties=hubspot_properties
                        )

                    table_info = {
                        "TABLE_SCHEMA": "hubspot",
                        "TABLE_NAME": table_name,
                        "TABLE_TYPE": "BASE TABLE",
                    }
                    tables_data.append(table_info)
                    logger.info(f"Table '{table_name}' is accessible")
                except Exception as access_error:
                    if "403" in str(access_error) or "MISSING_SCOPES" in str(access_error):
                        error_msg = _extract_hubspot_error_message(access_error)
                        logger.warning(f"Table '{table_name}' is not accessible: {error_msg}")
                    else:
                        logger.warning(f"Could not access table {table_name}: {str(access_error)}")

            if not tables_data:
                error_msg = (
                    "No HubSpot tables are accessible with your current access token. "
                    "Please ensure your token has the necessary scopes. "
                    "Update scopes at https://developers.hubspot.com/"
                )
                logger.error(error_msg)
                return Response(RESPONSE_TYPE.ERROR, error_message=error_msg)

            df = pd.DataFrame(tables_data)
            logger.info(f"Retrieved metadata for {len(tables_data)} accessible table(s)")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            error_msg = _extract_hubspot_error_message(e)
            logger.error(f"Failed to get tables: {error_msg}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve table list: {error_msg}")

    def get_columns(self, table_name: str) -> Response:
        """Return column information for a specific table."""
        valid_tables = [
            "companies",
            "contacts",
            "deals",
            "tickets",
            "tasks",
            "calls",
            "emails",
            "meetings",
            "notes",
        ]

        if table_name not in valid_tables:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Table '{table_name}' not found. Available tables: {', '.join(valid_tables)}",
            )

        try:
            self.connect()

            discovered_columns = self._get_default_discovered_columns(table_name)

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
        """Return column statistics for data catalog."""
        try:
            self.connect()

            all_tables = ["companies", "contacts", "deals", "tickets", "tasks", "calls", "emails", "meetings", "notes"]
            if table_names:
                tables_to_process = [t for t in table_names if t in all_tables]
            else:
                tables_to_process = all_tables

            all_statistics = []

            for table_name in tables_to_process:
                try:
                    table_statistics = []
                    default_properties = self._tables[table_name].get_columns()
                    hubspot_properties = [
                        to_hubspot_property(col)
                        for col in default_properties
                        if to_hubspot_property(col) != "hs_object_id"
                    ]

                    # Get sample data based on object type
                    if table_name in ["companies", "contacts", "deals", "tickets"]:
                        sample_data = list(
                            getattr(self.connection.crm, table_name).get_all(limit=1000, properties=hubspot_properties)
                        )
                    else:
                        sample_data = list(self._get_objects_all(table_name, limit=1000, properties=hubspot_properties))

                    if len(sample_data) > 0:
                        sample_size = len(sample_data)
                        logger.info(f"Calculating statistics from {sample_size} records for {table_name}")

                        all_properties = set()
                        for item in sample_data:
                            if hasattr(item, "properties") and item.properties:
                                all_properties.update(item.properties.keys())

                        # Statistics for 'id' column
                        id_values = [item.id for item in sample_data]
                        id_stats = self._calculate_column_statistics("id", id_values)
                        table_statistics.append(
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

                        for prop_name in sorted(all_properties):
                            column_name = to_internal_property(prop_name)

                            column_values = []
                            for item in sample_data:
                                if hasattr(item, "properties") and item.properties:
                                    column_values.append(item.properties.get(prop_name))
                                else:
                                    column_values.append(None)

                            stats = self._calculate_column_statistics(column_name, column_values)

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

                            table_statistics.append(
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

                        # Filter to only include default properties
                        table_statistics = [
                            col
                            for col in table_statistics
                            if col["COLUMN_NAME"] in default_properties or col["COLUMN_NAME"] == "id"
                        ]
                        all_statistics.extend(table_statistics)

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

    def _get_default_discovered_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get default discovered columns when API data is unavailable."""
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
        """Get default column metadata for data catalog when data is unavailable."""
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
            "tickets": "HubSpot tickets data including subject, status, priority and pipeline information",
            "tasks": "HubSpot tasks data including subject, status, priority and due dates",
            "calls": "HubSpot call logs including direction, duration, outcome and notes",
            "emails": "HubSpot email logs including subject, direction, status and content",
            "meetings": "HubSpot meeting logs including title, location, outcome and timing",
            "notes": "HubSpot notes for timeline entries on records",
        }
        return descriptions.get(table_name, f"HubSpot {table_name} data")

    def _estimate_table_rows(self, table_name: str) -> Optional[int]:
        """Get actual count of rows in a table using HubSpot Search API."""
        try:
            if table_name in ["companies", "contacts", "deals", "tickets"]:
                result = getattr(self.connection.crm, table_name).search_api.do_search(
                    public_object_search_request={"limit": 1}
                )
            else:
                result = self.connection.crm.objects.search_api.do_search(
                    table_name, public_object_search_request={"limit": 1}
                )
            return result.total if hasattr(result, "total") else None
        except Exception as e:
            logger.warning(f"Could not get row count for {table_name} using search API: {str(e)}")
        return None

    def _get_objects_all(
        self,
        object_type: str,
        limit: Optional[int] = None,
        properties: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> List[Any]:
        """Fetch objects with paging to honor custom limits for crm.objects."""
        results: List[Any] = []
        after = None
        page_max_size = 100

        if limit is None and "limit" in kwargs:
            limit = kwargs.pop("limit")
        if properties is None and "properties" in kwargs:
            properties = kwargs.pop("properties")

        while True:
            if limit is not None:
                remaining = limit - len(results)
                if remaining <= 0:
                    break
                page_size = min(page_max_size, remaining)
            else:
                page_size = page_max_size

            page = self.connection.crm.objects.basic_api.get_page(
                object_type, after=after, limit=page_size, properties=properties, **kwargs
            )
            results.extend(page.results)

            if page.paging is None:
                break
            after = page.paging.next.after

        return results

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
            try:
                s = pd.Series(non_null_values)
                if pd_types.is_numeric_dtype(s):
                    avg = s.mean()
                    stats["average_value"] = round(avg, 2)
            except (ValueError, TypeError):
                pass

        return stats

    def _infer_data_type_from_samples(self, values: List[Any]) -> str:
        """Infer data type from multiple sample values for better accuracy."""
        non_null_values = [v for v in values if v is not None]

        if not non_null_values:
            return "VARCHAR"

        type_counts = {}
        for value in non_null_values[:100]:
            inferred_type = self._infer_data_type(value)
            type_counts[inferred_type] = type_counts.get(inferred_type, 0) + 1

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

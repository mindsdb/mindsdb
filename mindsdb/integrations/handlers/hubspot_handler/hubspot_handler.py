from collections import Counter
from typing import Optional, List, Dict, Any, Tuple
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
    LeadsTable,
    OwnersTable,
    DealStagesTable,
    to_hubspot_property,
    to_internal_property,
    HUBSPOT_TABLE_COLUMN_DEFINITIONS,
)
from mindsdb.integrations.handlers.hubspot_handler.hubspot_association_tables import (
    ASSOCIATION_TABLE_CLASSES,
)
from mindsdb.integrations.handlers.hubspot_handler.hubspot_association_utils import (
    PRIMARY_ASSOCIATIONS_CONFIG,
)
from mindsdb.integrations.libs.api_handler import MetaAPIHandler
from mindsdb.integrations.utilities.sql_utils import FilterCondition, FilterOperator, extract_comparison_conditions

from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.api.mysql.mysql_proxy.libs.constants.mysql import MYSQL_DATA_TYPE
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql
from mindsdb_sql_parser.ast import Select, Identifier, BinaryOperation, Star
from mindsdb_sql_parser.ast import Join as SQLJoin


logger = log.getLogger(__name__)

# Maps (from_table, to_table) → (association_table_name, from_id_col, to_id_col)
# Used to suggest the correct association-table pattern when users write direct FK joins.
_DIRECT_JOIN_ASSOC_MAP = {
    ("companies", "contacts"): ("company_contacts", "company_id", "contact_id"),
    ("companies", "deals"): ("company_deals", "company_id", "deal_id"),
    ("companies", "tickets"): ("company_tickets", "company_id", "ticket_id"),
    ("contacts", "companies"): ("contact_companies", "contact_id", "company_id"),
    ("contacts", "deals"): ("contact_deals", "contact_id", "deal_id"),
    ("contacts", "tickets"): ("contact_tickets", "contact_id", "ticket_id"),
    ("deals", "companies"): ("deal_companies", "deal_id", "company_id"),
    ("deals", "contacts"): ("deal_contacts", "deal_id", "contact_id"),
    ("tickets", "companies"): ("ticket_companies", "ticket_id", "company_id"),
    ("tickets", "contacts"): ("ticket_contacts", "ticket_id", "contact_id"),
    ("tickets", "deals"): ("ticket_deals", "ticket_id", "deal_id"),
}


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
        self._association_tables = set(ASSOCIATION_TABLE_CLASSES.keys())
        self._non_object_tables = {"owners", "deal_stages"}

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
        self._register_table("leads", LeadsTable(self))
        self._register_table("owners", OwnersTable(self))
        self._register_table("deal_stages", DealStagesTable(self))

        for table_name, table_class in ASSOCIATION_TABLE_CLASSES.items():
            self._register_table(table_name, table_class(self))

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
        logger.debug(f"[HubSpotHandler] native_query() called — query: {query}")
        if not query:
            return Response(RESPONSE_TYPE.ERROR, error_message="Query cannot be None or empty")

        try:
            ast = parse_sql(query)
        except Exception as e:
            logger.error(f"Failed to execute native query: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Query execution failed: {str(e)}")

        try:
            if isinstance(ast, Select) and isinstance(ast.from_table, SQLJoin):
                logger.debug("[HubSpotHandler] native_query() — routing to _execute_join_query")
                return self._execute_join_query(ast)
            logger.debug("[HubSpotHandler] native_query() — routing to query()")
            return self.query(ast)
        except Exception as e:
            logger.error(f"Failed to execute native query: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Query execution failed: {str(e)}")

    CORE_TABLES = frozenset(
        {"companies", "contacts", "deals", "tickets", "tasks", "calls", "emails", "meetings", "notes"}
    )

    def get_tables(self) -> Response:
        """Return list of tables available in the HubSpot integration."""
        try:
            self.connect()

            tables_data = []
            all_tables = list(self._tables.keys())
            for table_name in all_tables:
                try:
                    if table_name in self._association_tables:
                        table_info = {
                            "TABLE_SCHEMA": "hubspot",
                            "TABLE_NAME": table_name,
                            "TABLE_TYPE": "BASE TABLE",
                        }
                        tables_data.append(table_info)
                        continue
                    if table_name in self._non_object_tables:
                        self._tables[table_name].list(limit=1)
                        table_info = {
                            "TABLE_SCHEMA": "hubspot",
                            "TABLE_NAME": table_name,
                            "TABLE_TYPE": "BASE TABLE",
                        }
                        tables_data.append(table_info)
                        continue

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
        valid_tables = list(self._tables.keys())

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

            all_tables = [
                name
                for name in self._tables.keys()
                if name not in self._association_tables and name not in self._non_object_tables
            ]
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
        if (
            table_name in self._association_tables or table_name in self._non_object_tables
        ) and table_name in HUBSPOT_TABLE_COLUMN_DEFINITIONS:
            base_columns = []
            ordinal_position = 1
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
        if (
            table_name in self._association_tables or table_name in self._non_object_tables
        ) and table_name in HUBSPOT_TABLE_COLUMN_DEFINITIONS:
            base_columns = []
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
            "company_contacts": "HubSpot company to contact associations",
            "company_deals": "HubSpot company to deal associations",
            "company_tickets": "HubSpot company to ticket associations",
            "contact_companies": "HubSpot contact to company associations",
            "contact_deals": "HubSpot contact to deal associations",
            "contact_tickets": "HubSpot contact to ticket associations",
            "deal_companies": "HubSpot deal to company associations",
            "deal_contacts": "HubSpot deal to contact associations",
            "ticket_companies": "HubSpot ticket to company associations",
            "ticket_contacts": "HubSpot ticket to contact associations",
            "ticket_deals": "HubSpot ticket to deal associations",
            "owners": "HubSpot owners with names and emails",
            "deal_stages": "HubSpot deal pipeline stages with labels",
            "leads": "HubSpot leads data including lead status, source and other lead properties",
        }
        return descriptions.get(table_name, f"HubSpot {table_name} data")

    def _estimate_table_rows(self, table_name: str) -> Optional[int]:
        """Get actual count of rows in a table using HubSpot Search API."""
        try:
            if table_name in ["companies", "contacts", "deals", "tickets", "leads"]:
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

    def _rewrite_where_for_table(self, where_node: Any, table_alias: str, is_main_table: bool = False) -> Any:
        """Extract WHERE conditions for a specific table alias, stripping the alias prefix.

        Returns a new WHERE AST node with aliases stripped, or None if no conditions
        reference the given alias.
        """
        if where_node is None:
            return None

        if isinstance(where_node, BinaryOperation):
            if where_node.op.lower() == "and":
                left = self._rewrite_where_for_table(where_node.args[0], table_alias, is_main_table)
                right = self._rewrite_where_for_table(where_node.args[1], table_alias, is_main_table)
                if left is not None and right is not None:
                    return BinaryOperation("and", args=[left, right])
                return left if left is not None else right
            else:
                # Leaf comparison node
                arg0 = where_node.args[0] if where_node.args else None
                if isinstance(arg0, Identifier):
                    parts = arg0.parts
                    if len(parts) >= 2:
                        alias = parts[0].lower()
                        col = parts[-1]
                        if alias == table_alias.lower():
                            new_args = [Identifier(col)] + list(where_node.args[1:])
                            return BinaryOperation(where_node.op, args=new_args)
                        return None
                    elif len(parts) == 1 and is_main_table:
                        # Unqualified condition — assign to the main (FROM) table
                        return where_node
        return None

    def _format_select_targets(self, targets) -> str:
        """Render SELECT target list back to a SQL string fragment."""
        if not targets:
            return "*"
        parts = []
        for t in targets:
            if isinstance(t, Star):
                parts.append("*")
            elif isinstance(t, Identifier):
                parts.append(".".join(str(p) for p in t.parts))
        return ", ".join(parts) if parts else "*"

    def _suggest_association_query(
        self, ast: Select, left_name: str, left_alias: str, right_name: str, right_alias: str
    ) -> Response:
        """Return a helpful error directing the user to use association tables.

        Analyses the WHERE clause to determine which table is being filtered so the
        suggestion puts the filtered table first (making the join efficient).
        """
        # Decide which table is "filtered" (has WHERE conditions) — put it first
        where_on_right = self._rewrite_where_for_table(ast.where, right_alias) is not None
        where_on_left = self._rewrite_where_for_table(ast.where, left_alias, is_main_table=True) is not None

        if where_on_right and not where_on_left:
            from_name, from_alias = right_name, right_alias
            to_name, to_alias = left_name, left_alias
        else:
            from_name, from_alias = left_name, left_alias
            to_name, to_alias = right_name, right_alias

        assoc_info = _DIRECT_JOIN_ASSOC_MAP.get((from_name, to_name))
        if assoc_info is None:
            # Try the reverse direction
            assoc_info = _DIRECT_JOIN_ASSOC_MAP.get((to_name, from_name))

        if assoc_info is None:
            error_msg = (
                f"Direct JOINs between '{left_name}' and '{right_name}' are not supported. "
                "Please use HubSpot association tables to join these objects."
            )
            return Response(RESPONSE_TYPE.ERROR, error_message=error_msg)  # type: ignore[arg-type]

        assoc_table, from_id_col, to_id_col = assoc_info
        assoc_alias = assoc_table[:2]  # short alias, e.g. "cc" for company_contacts

        col_str = self._format_select_targets(ast.targets)
        where_clause = f"\nWHERE {ast.where}" if ast.where else ""
        limit_clause = f"\nLIMIT {ast.limit.value}" if ast.limit else ""

        suggested = (
            f"SELECT {col_str}\n"
            f"FROM `my_hubspot`.{from_name} {from_alias}\n"
            f"JOIN `my_hubspot`.{assoc_table} {assoc_alias} "
            f"ON {assoc_alias}.{from_id_col} = {from_alias}.id\n"
            f"JOIN `my_hubspot`.{to_name} {to_alias} "
            f"ON {to_alias}.id = {assoc_alias}.{to_id_col}"
            f"{where_clause}"
            f"{limit_clause}"
        )

        error_msg = (
            f"Direct JOINs between HubSpot objects using foreign key columns (e.g. primary_company_id) "
            f"are not supported. The HubSpot API represents relationships through association tables.\n\n"
            f"Please rewrite your query using the '{assoc_table}' association table:\n\n"
            f"{suggested}"
        )
        return Response(RESPONSE_TYPE.ERROR, error_message=error_msg)

    def _flatten_join_tree(self, from_node) -> List[Tuple[str, str, Any]]:
        """Flatten nested Join AST nodes into an ordered list of (table_name, alias, on_condition)."""

        result: List[Tuple[str, str, Any]] = []

        def _get_alias(ident: Identifier) -> str:
            alias = getattr(ident, "alias", None)
            if alias is None:
                return ident.parts[-1].lower()
            if isinstance(alias, str):
                return alias.lower()
            return alias.parts[-1].lower()

        def _walk(node, on_cond=None):
            if isinstance(node, SQLJoin):
                _walk(node.left, None)
                right_name = node.right.parts[-1].lower()
                right_alias = _get_alias(node.right)
                result.append((right_name, right_alias, node.condition))
            elif isinstance(node, Identifier):
                result.append((node.parts[-1].lower(), _get_alias(node), on_cond))

        _walk(from_node)
        return result

    def _execute_join_query(self, ast: Select) -> Response:
        """Execute a JOIN query using the HubSpot associations API."""
        logger.debug(f"[HubSpotHandler] _execute_join_query() called")

        tables = self._flatten_join_tree(ast.from_table)
        if len(tables) < 2 or len(tables) > 3:
            return Response(
                RESPONSE_TYPE.ERROR, error_message="Only 2- and 3-table joins via association tables are supported."
            )

        alias_to_name: Dict[str, str] = {alias: name for name, alias, _ in tables}
        is_main_alias = tables[0][1]

        def _parse_on(on_cond) -> Optional[Tuple[Optional[str], str, Optional[str], str]]:
            """Parse an ON equality into (left_alias, left_col, right_alias, right_col) or None."""
            if not isinstance(on_cond, BinaryOperation) or on_cond.op != "=":
                return None
            a, b = on_cond.args
            if not (isinstance(a, Identifier) and isinstance(b, Identifier)):
                return None

            def _split(ident):
                parts = ident.parts
                return (parts[0].lower(), parts[-1].lower()) if len(parts) >= 2 else (None, parts[0].lower())

            la, lc = _split(a)
            ra, rc = _split(b)
            return la, lc, ra, rc

        if len(tables) == 2:
            (t1_name, t1_alias, _), (t2_name, t2_alias, on2) = tables

            # Reject direct core+core joins
            if t1_name in self.CORE_TABLES and t2_name in self.CORE_TABLES:
                return self._suggest_association_query(ast, t1_name, t1_alias, t2_name, t2_alias)

            parsed = _parse_on(on2)
            if not parsed:
                return Response(RESPONSE_TYPE.ERROR, error_message="Unsupported JOIN condition.")

            la, lc, ra, rc = parsed
            if t1_name in self._association_tables:
                assoc_name, assoc_alias, assoc_col = t1_name, t1_alias, lc if la == t1_alias else rc
                core_name, core_alias, core_col = t2_name, t2_alias, rc if la == t1_alias else lc
            else:
                assoc_name, assoc_alias, assoc_col = t2_name, t2_alias, rc if ra == t2_alias else lc
                core_name, core_alias, core_col = t1_name, t1_alias, lc if ra == t2_alias else rc

            if assoc_name not in self._tables or core_name not in self._tables:
                return Response(RESPONSE_TYPE.ERROR, error_message="Unknown table in JOIN.")

            limit = ast.limit.value if ast.limit else None
            core_where = self._rewrite_where_for_table(
                ast.where, core_alias, is_main_table=(core_alias == is_main_alias)
            )
            assoc_where = self._rewrite_where_for_table(
                ast.where, assoc_alias, is_main_table=(assoc_alias == is_main_alias)
            )

            # Fetch core table (filtered side)
            core_df = self._tables[core_name].select(
                Select(targets=[Star()], from_table=Identifier(core_name), where=core_where)
            )
            if core_df.empty or core_col not in core_df.columns:
                return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

            core_ids = core_df[core_col].dropna().astype(str).tolist()
            if not core_ids:
                return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

            # Fetch association table filtered by core ids
            assoc_conditions: List[FilterCondition] = [FilterCondition(assoc_col, FilterOperator.IN, core_ids)]
            if assoc_where is not None:
                try:
                    for cond in extract_comparison_conditions(assoc_where):
                        assoc_conditions.append(FilterCondition(cond[1], FilterOperator(cond[0].upper()), cond[2]))
                except Exception:
                    pass

            assoc_df = self._tables[assoc_name].list(conditions=assoc_conditions, limit=limit)
            if assoc_df.empty:
                return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

            merged = assoc_df.merge(
                core_df, left_on=assoc_col, right_on=core_col, how="inner", suffixes=("", f"_{core_alias}")
            )
            result_df = self._resolve_select_targets(ast.targets, merged, alias_to_name, tables)
            if limit:
                result_df = result_df.head(limit)
            return Response(RESPONSE_TYPE.TABLE, data_frame=result_df)

        (t1_name, t1_alias, _), (t2_name, t2_alias, on2), (t3_name, t3_alias, on3) = tables

        if t2_name not in self._association_tables:
            if t1_name in self.CORE_TABLES and t2_name in self.CORE_TABLES:
                return self._suggest_association_query(ast, t1_name, t1_alias, t2_name, t2_alias)
            return Response(RESPONSE_TYPE.ERROR, error_message="Only CORE JOIN ASSOC JOIN CORE pattern is supported.")

        parsed2 = _parse_on(on2)
        parsed3 = _parse_on(on3)
        if not parsed2 or not parsed3:
            return Response(RESPONSE_TYPE.ERROR, error_message="Unsupported JOIN condition — expected simple equality.")

        la2, lc2, ra2, rc2 = parsed2
        t1_fk_col = rc2 if la2 == t2_alias else lc2
        t1_pk_col = lc2 if la2 == t1_alias else rc2

        la3, lc3, ra3, rc3 = parsed3
        t3_fk_col = lc3 if la3 == t2_alias else rc3
        t3_pk_col = rc3 if la3 == t2_alias else lc3

        if t1_name not in self._tables or t2_name not in self._tables or t3_name not in self._tables:
            return Response(RESPONSE_TYPE.ERROR, error_message="Unknown table in JOIN.")

        limit = ast.limit.value if ast.limit else None
        t1_where = self._rewrite_where_for_table(ast.where, t1_alias, is_main_table=(t1_alias == is_main_alias))
        t2_where = self._rewrite_where_for_table(ast.where, t2_alias, is_main_table=(t2_alias == is_main_alias))
        t3_where = self._rewrite_where_for_table(ast.where, t3_alias, is_main_table=(t3_alias == is_main_alias))

        t1_df = self._tables[t1_name].select(Select(targets=[Star()], from_table=Identifier(t1_name), where=t1_where))
        if t1_df.empty or t1_pk_col not in t1_df.columns:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        t1_ids = t1_df[t1_pk_col].dropna().astype(str).tolist()
        if not t1_ids:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        assoc_conditions: List[FilterCondition] = [FilterCondition(t1_fk_col, FilterOperator.IN, t1_ids)]
        if t2_where is not None:
            try:
                for cond in extract_comparison_conditions(t2_where):
                    assoc_conditions.append(FilterCondition(cond[1], FilterOperator(cond[0].upper()), cond[2]))
            except Exception:
                pass

        t2_df = self._tables[t2_name].list(conditions=assoc_conditions)
        if t2_df.empty or t3_fk_col not in t2_df.columns:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        t3_ids = t2_df[t3_fk_col].dropna().astype(str).tolist()
        if not t3_ids:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        t3_conditions: List[FilterCondition] = [FilterCondition(t3_pk_col, FilterOperator.IN, t3_ids)]
        if t3_where is not None:
            try:
                for cond in extract_comparison_conditions(t3_where):
                    t3_conditions.append(FilterCondition(cond[1], FilterOperator(cond[0].upper()), cond[2]))
            except Exception:
                pass

        t3_df = self._tables[t3_name].list(conditions=t3_conditions)
        if t3_df.empty:
            return Response(RESPONSE_TYPE.TABLE, data_frame=pd.DataFrame())

        merged = t2_df.merge(t1_df, left_on=t1_fk_col, right_on=t1_pk_col, how="inner", suffixes=("", f"_{t1_alias}"))
        merged = merged.merge(t3_df, left_on=t3_fk_col, right_on=t3_pk_col, how="inner", suffixes=("", f"_{t3_alias}"))

        result_df = self._resolve_select_targets(ast.targets, merged, alias_to_name, tables)
        if limit:
            result_df = result_df.head(limit)
        return Response(RESPONSE_TYPE.TABLE, data_frame=result_df)

    def _resolve_select_targets(
        self,
        targets,
        df: pd.DataFrame,
        alias_to_name: Dict[str, str],
        join_tables: List[Tuple[str, str, Any]],
    ) -> pd.DataFrame:
        """Resolve SELECT target list against a merged DataFrame.

        Handles qualified names (alias.col), unqualified names, and Star.
        Returns a DataFrame with only the requested columns (renamed to alias.col if needed).
        """
        if not targets:
            return df

        cols: List[str] = []
        renames: Dict[str, str] = {}
        has_star = any(isinstance(t, Star) for t in targets)
        if has_star:
            return df

        for t in targets:
            if isinstance(t, Identifier):
                alias = getattr(t, "alias", None)
                parts = t.parts
                if len(parts) >= 2:
                    tbl_alias, col = parts[0].lower(), parts[-1]
                    if col in df.columns:
                        cols.append(col)
                        if alias:
                            renames[col] = alias
                    else:
                        # Try suffixed variant (e.g. "id_co")
                        suffixed = f"{col}_{tbl_alias}"
                        if suffixed in df.columns:
                            cols.append(suffixed)
                            renames[suffixed] = alias or col
                else:
                    col = parts[0]
                    if col in df.columns:
                        cols.append(col)
                        if alias:
                            renames[col] = alias

        available = list(dict.fromkeys(c for c in cols if c in df.columns))
        if available:
            df = df[available]
        if renames:
            df = df.rename(columns=renames)
        return df.reset_index(drop=True)

    def meta_get_primary_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """Return primary key metadata for the data catalog.

        Every object table has ``id`` as its PK.
        Association tables have a composite PK on both ID columns.
        """
        try:
            self.connect()
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve primary keys: {e}")

        all_tables = list(self._tables.keys())
        if table_names:
            all_tables = [t for t in all_tables if t in table_names]

        rows: List[Dict[str, Any]] = []

        for table_name in all_tables:
            if table_name in self._association_tables:
                id_cols = [c for c in self._tables[table_name].get_columns() if c.endswith("_id")]
                for pos, col in enumerate(id_cols, start=1):
                    rows.append(
                        {
                            "TABLE_NAME": table_name,
                            "COLUMN_NAME": col,
                            "ORDINAL_POSITION": pos,
                            "CONSTRAINT_NAME": f"pk_{table_name}",
                        }
                    )
            elif table_name == "deal_stages":
                for pos, col in enumerate(["pipeline_id", "stage_id"], start=1):
                    rows.append(
                        {
                            "TABLE_NAME": table_name,
                            "COLUMN_NAME": col,
                            "ORDINAL_POSITION": pos,
                            "CONSTRAINT_NAME": f"pk_{table_name}",
                        }
                    )
            else:
                rows.append(
                    {
                        "TABLE_NAME": table_name,
                        "COLUMN_NAME": "id",
                        "ORDINAL_POSITION": 1,
                        "CONSTRAINT_NAME": f"pk_{table_name}",
                    }
                )

        df = (
            pd.DataFrame(rows)
            if rows
            else pd.DataFrame(columns=["TABLE_NAME", "COLUMN_NAME", "ORDINAL_POSITION", "CONSTRAINT_NAME"])
        )
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

    def meta_get_foreign_keys(self, table_names: Optional[List[str]] = None) -> Response:
        """Return foreign key metadata for the data catalog.

        Exposes two sets of relationships so the agent can generate correct JOINs:

        1. Association table FKs — e.g. company_contacts.company_id → companies.id
        2. Object-table primary_*_id FKs — e.g. contacts.primary_company_id → companies.id
        """
        try:
            self.connect()
        except Exception as e:
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve foreign keys: {e}")

        _ASSOC_TARGET_TO_TABLE = {
            "companies": "companies",
            "contacts": "contacts",
            "deals": "deals",
            "tickets": "tickets",
        }

        all_tables = set(self._tables.keys())
        if table_names:
            all_tables = set(table_names).intersection(all_tables)

        rows: List[Dict[str, Any]] = []

        # 1. Association table FKs — aggregated from each table's meta_get_foreign_keys()
        for table_name in sorted(all_tables):
            if table_name not in self._association_tables:
                continue
            table_obj = self._tables[table_name]
            if hasattr(table_obj, "meta_get_foreign_keys"):
                for fk in table_obj.meta_get_foreign_keys(table_name):
                    col = fk.get("COLUMN_NAME")
                    rows.append(
                        {
                            "CHILD_TABLE_NAME": fk.get("TABLE_NAME", table_name),
                            "CHILD_COLUMN_NAME": col,
                            "PARENT_TABLE_NAME": fk.get("REFERENCED_TABLE_NAME"),
                            "PARENT_COLUMN_NAME": fk.get("REFERENCED_COLUMN_NAME", "id"),
                            "CONSTRAINT_NAME": f"fk_{table_name}_{col}",
                        }
                    )

        for table_name in sorted(all_tables):
            if table_name in self._association_tables or table_name in self._non_object_tables:
                continue
            for target_type, column_name in PRIMARY_ASSOCIATIONS_CONFIG.get(table_name, []):
                parent_table = _ASSOC_TARGET_TO_TABLE.get(target_type)
                if parent_table is None:
                    continue
                rows.append(
                    {
                        "CHILD_TABLE_NAME": table_name,
                        "CHILD_COLUMN_NAME": column_name,
                        "PARENT_TABLE_NAME": parent_table,
                        "PARENT_COLUMN_NAME": "id",
                        "CONSTRAINT_NAME": f"fk_{table_name}_{column_name}",
                    }
                )

        df = (
            pd.DataFrame(rows)
            if rows
            else pd.DataFrame(
                columns=[
                    "CHILD_TABLE_NAME",
                    "CHILD_COLUMN_NAME",
                    "PARENT_TABLE_NAME",
                    "PARENT_COLUMN_NAME",
                    "CONSTRAINT_NAME",
                ]
            )
        )
        return Response(RESPONSE_TYPE.TABLE, data_frame=df)

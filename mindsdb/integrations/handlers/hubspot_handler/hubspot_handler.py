from typing import Optional, List, Dict, Any
import pandas as pd
from hubspot import HubSpot

from mindsdb.integrations.handlers.hubspot_handler.hubspot_tables import ContactsTable, CompaniesTable, DealsTable
from mindsdb.integrations.libs.api_handler import APIHandler
from mindsdb.integrations.libs.response import (
    HandlerStatusResponse as StatusResponse,
    HandlerResponse as Response,
    RESPONSE_TYPE,
)
from mindsdb.utilities import log
from mindsdb_sql_parser import parse_sql

logger = log.getLogger(__name__)


class HubspotHandler(APIHandler):
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
                list(self.connection.crm.companies.get_all(limit=1))

            response.success = True
            logger.info("HubSpot connection check successful")

        except Exception as e:
            logger.error("HubSpot connection check failed")
            response.error_message = str(e)
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

            # Get basic table information
            tables_data = []

            for table_name in ["companies", "contacts", "deals"]:
                try:
                    table_info = {
                        "TABLE_NAME": table_name,
                        "TABLE_TYPE": "BASE TABLE",
                        "TABLE_SCHEMA": "hubspot",
                        "TABLE_DESCRIPTION": self._get_table_description(table_name),
                        "ROW_COUNT": self._estimate_table_rows(table_name),
                    }
                    tables_data.append(table_info)

                except Exception as e:
                    logger.warning(f"Could not get metadata for table {table_name}: {str(e)}")

                    tables_data.append(
                        {
                            "TABLE_NAME": table_name,
                            "TABLE_TYPE": "BASE TABLE",
                            "TABLE_SCHEMA": "hubspot",
                            "TABLE_DESCRIPTION": self._get_table_description(table_name),
                            "ROW_COUNT": None,
                        }
                    )

            df = pd.DataFrame(tables_data)
            logger.info(f"Retrieved metadata for {len(tables_data)} tables")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Failed to get tables: {str(e)}")
            return Response(RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve table list: {str(e)}")

    def get_columns(self, table_name: str) -> Response:
        """Return column information for a specific table.

        Args:
            table_name (str): Name of the table to get column information for

        Returns:
            Response: A response containing column metadata including names, types,
                     descriptions, and statistics.
        """
        if table_name not in ["companies", "contacts", "deals"]:
            return Response(
                RESPONSE_TYPE.ERROR,
                error_message=f"Table '{table_name}' not found. Available tables: companies, contacts, deals",
            )

        try:
            self.connect()

            columns_data = self._get_columns_with_statistics(table_name)

            df = pd.DataFrame(columns_data)
            logger.info(f"Retrieved {len(columns_data)} columns for table {table_name}")
            return Response(RESPONSE_TYPE.TABLE, data_frame=df)

        except Exception as e:
            logger.error(f"Failed to get columns for table {table_name}: {str(e)}")
            return Response(
                RESPONSE_TYPE.ERROR, error_message=f"Failed to retrieve columns for table '{table_name}': {str(e)}"
            )

    def _get_table_description(self, table_name: str) -> str:
        """Get description for a table."""
        descriptions = {
            "companies": "HubSpot companies data including name, industry, location and other company properties",
            "contacts": "HubSpot contacts data including email, name, phone and other contact properties",
            "deals": "HubSpot deals data including deal name, amount, stage and other deal properties",
        }
        return descriptions.get(table_name, f"HubSpot {table_name} data")

    def _estimate_table_rows(self, table_name: str) -> Optional[int]:
        """Estimate number of rows in a table using HubSpot API."""
        try:
            if table_name == "companies":
                companies = self.connection.crm.companies.get_all(limit=100)
                return len(list(companies)) if companies else 0
            elif table_name == "contacts":
                contacts = self.connection.crm.contacts.get_all(limit=100)
                return len(list(contacts)) if contacts else 0
            elif table_name == "deals":
                deals = self.connection.crm.deals.get_all(limit=100)
                return len(list(deals)) if deals else 0
        except Exception as e:
            logger.warning(f"Could not estimate rows for {table_name}: {str(e)}")
        return None

    def _get_columns_with_statistics(self, table_name: str) -> List[Dict[str, Any]]:
        """Get detailed column information with comprehensive statistics."""
        try:
            sample_data = None

            if table_name == "companies":
                sample_data = list(self.connection.crm.companies.get_all(limit=1000))
            elif table_name == "contacts":
                sample_data = list(self.connection.crm.contacts.get_all(limit=1000))
            elif table_name == "deals":
                sample_data = list(self.connection.crm.deals.get_all(limit=1000))

            columns_info = []

            if sample_data and len(sample_data) > 0:
                sample_size = len(sample_data)
                logger.info(f"Analyzing {sample_size} records for {table_name} column statistics")

                all_properties = set()
                for item in sample_data:
                    if hasattr(item, "properties") and item.properties:
                        all_properties.update(item.properties.keys())

                id_stats = self._calculate_column_statistics("id", [item.id for item in sample_data])
                columns_info.append(
                    {
                        "COLUMN_NAME": "id",
                        "DATA_TYPE": "VARCHAR",
                        "IS_NULLABLE": False,
                        "COLUMN_DEFAULT": None,
                        "COLUMN_DESCRIPTION": "Unique identifier for the record (Primary Key)",
                        "IS_PRIMARY_KEY": True,
                        "IS_FOREIGN_KEY": False,
                        "NULL_COUNT": id_stats["null_count"],
                        "DISTINCT_COUNT": id_stats["distinct_count"],
                        "MIN_VALUE": id_stats["min_value"],
                        "MAX_VALUE": id_stats["max_value"],
                        "AVERAGE_VALUE": id_stats.get("average_value"),
                    }
                )

                for prop_name in sorted(all_properties):
                    column_name = prop_name
                    if prop_name == "hs_lastmodifieddate":
                        column_name = "lastmodifieddate"

                    column_values = []
                    for item in sample_data:
                        if hasattr(item, "properties") and item.properties:
                            value = item.properties.get(prop_name)
                            column_values.append(value)
                        else:
                            column_values.append(None)

                    stats = self._calculate_column_statistics(prop_name, column_values)
                    data_type = self._infer_data_type_from_samples(column_values)

                    is_foreign_key = self._is_potential_foreign_key(prop_name, column_values)

                    columns_info.append(
                        {
                            "COLUMN_NAME": column_name,
                            "DATA_TYPE": data_type,
                            "IS_NULLABLE": True,
                            "COLUMN_DEFAULT": None,
                            "COLUMN_DESCRIPTION": f"HubSpot property: {prop_name}",
                            "IS_PRIMARY_KEY": False,
                            "IS_FOREIGN_KEY": is_foreign_key,
                            "NULL_COUNT": stats["null_count"],
                            "DISTINCT_COUNT": stats["distinct_count"],
                            "MIN_VALUE": stats["min_value"],
                            "MAX_VALUE": stats["max_value"],
                            "AVERAGE_VALUE": stats["average_value"],
                        }
                    )

            if not columns_info:
                columns_info = self._get_default_columns_with_stats(table_name)

            return columns_info

        except Exception as e:
            logger.warning(f"Could not analyze column statistics for {table_name}: {str(e)}")
            return self._get_default_columns_with_stats(table_name)

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
            str_values = [str(v) for v in non_null_values]
            stats["min_value"] = min(str_values)
            stats["max_value"] = max(str_values)

            # Try to calculate numeric average for numeric columns
            try:
                numeric_values = []
                for v in non_null_values:
                    if isinstance(v, (int, float)):
                        numeric_values.append(float(v))
                    elif isinstance(v, str) and v.replace(".", "").replace("-", "").isdigit():
                        numeric_values.append(float(v))

                if numeric_values:
                    stats["average_value"] = round(sum(numeric_values) / len(numeric_values), 2)
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

    def _is_potential_foreign_key(self, prop_name: str, values: List[Any]) -> bool:
        """Determine if a column might be a foreign key based on naming and patterns."""
        # Common foreign key naming patterns
        fk_patterns = [
            "_id",
            "id_",
            "owner_id",
            "company_id",
            "contact_id",
            "deal_id",
            "hubspot_owner_id",
            "associated_",
        ]

        # Check if column name suggests it's a foreign key
        prop_lower = prop_name.lower()
        name_suggests_fk = any(pattern in prop_lower for pattern in fk_patterns)

        # Check if values look like IDs (numeric or UUID-like)
        non_null_values = [v for v in values[:50] if v is not None]  # Sample first 50
        if not non_null_values:
            return False

        # Count how many values look like IDs
        id_like_count = 0
        for value in non_null_values:
            str_value = str(value)
            # Check if it's numeric or looks like a UUID/ID
            if (
                str_value.isdigit()
                or len(str_value) > 10  # Long strings might be IDs
                or "-" in str_value
                or "_" in str_value  # IDs might have underscores like "owner_123"
            ):
                id_like_count += 1

        # Calculate percentage of values that look like IDs
        values_suggest_fk = (id_like_count / len(non_null_values)) > 0.5  # More lenient threshold

        # If name strongly suggests FK (ends with _id or starts with id_) and at least some values look like IDs
        if name_suggests_fk and (prop_lower.endswith("_id") or prop_lower.startswith("id_")):
            return values_suggest_fk

        # Otherwise require both name and values to suggest FK with higher threshold
        return name_suggests_fk and (id_like_count / len(non_null_values)) > 0.7

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
            # Check if it looks like a datetime
            if "T" in value and ("Z" in value or "+" in value):
                return "TIMESTAMP"
            return "VARCHAR"
        else:
            return "VARCHAR"

    def _create_column_def(
        self,
        name: str,
        data_type: str,
        description: str,
        is_nullable: bool = True,
        is_primary_key: bool = False,
        is_foreign_key: bool = False,
        null_count: int = None,
    ) -> Dict[str, Any]:
        """Helper to create a column definition with defaults."""
        return {
            "COLUMN_NAME": name,
            "DATA_TYPE": data_type,
            "IS_NULLABLE": is_nullable,
            "COLUMN_DEFAULT": None,
            "COLUMN_DESCRIPTION": description,
            "IS_PRIMARY_KEY": is_primary_key,
            "IS_FOREIGN_KEY": is_foreign_key,
            "NULL_COUNT": null_count,
            "DISTINCT_COUNT": None,
            "MIN_VALUE": None,
            "MAX_VALUE": None,
            "AVERAGE_VALUE": None,
        }

    def _get_default_columns_with_stats(self, table_name: str) -> List[Dict[str, Any]]:
        """Get default column definitions with statistics when sample data is not available."""
        # Base columns common to all tables
        base_columns = [
            self._create_column_def(
                "id", "VARCHAR", "Unique identifier (Primary Key)", is_nullable=False, is_primary_key=True, null_count=0
            ),
            self._create_column_def("createdate", "TIMESTAMP", "Creation date"),
            self._create_column_def("lastmodifieddate", "TIMESTAMP", "Last modification date"),
        ]

        # Table-specific columns
        table_columns = {
            "companies": [
                ("name", "VARCHAR", "Company name"),
                ("domain", "VARCHAR", "Company domain"),
                ("industry", "VARCHAR", "Industry"),
                ("city", "VARCHAR", "City"),
                ("state", "VARCHAR", "State"),
                ("phone", "VARCHAR", "Phone number"),
            ],
            "contacts": [
                ("email", "VARCHAR", "Email address"),
                ("firstname", "VARCHAR", "First name"),
                ("lastname", "VARCHAR", "Last name"),
                ("phone", "VARCHAR", "Phone number"),
                ("company", "VARCHAR", "Associated company", True),  # is_foreign_key=True
                ("website", "VARCHAR", "Website URL"),
            ],
            "deals": [
                ("dealname", "VARCHAR", "Deal name"),
                ("amount", "DECIMAL", "Deal amount"),
                ("dealstage", "VARCHAR", "Deal stage"),
                ("pipeline", "VARCHAR", "Sales pipeline"),
                ("closedate", "DATE", "Expected close date"),
                ("hubspot_owner_id", "VARCHAR", "Owner ID", True),  # is_foreign_key=True
            ],
        }

        # Add table-specific columns
        if table_name in table_columns:
            for col_spec in table_columns[table_name]:
                name, data_type, description = col_spec[:3]
                is_foreign_key = col_spec[3] if len(col_spec) > 3 else False
                base_columns.append(
                    self._create_column_def(name, data_type, description, is_foreign_key=is_foreign_key)
                )

        return base_columns

    def _get_default_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Legacy method - calls new stats method and strips extra fields for backward compatibility."""
        columns_with_stats = self._get_default_columns_with_stats(table_name)

        # Strip statistics fields for backward compatibility
        legacy_columns = []
        for col in columns_with_stats:
            legacy_col = {
                "COLUMN_NAME": col["COLUMN_NAME"],
                "DATA_TYPE": col["DATA_TYPE"],
                "IS_NULLABLE": col["IS_NULLABLE"],
                "COLUMN_DEFAULT": col["COLUMN_DEFAULT"],
                "COLUMN_DESCRIPTION": col["COLUMN_DESCRIPTION"],
            }
            legacy_columns.append(legacy_col)

        return legacy_columns

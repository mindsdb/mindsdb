import pandas as pd
from abc import abstractmethod
from typing import List, Dict, Tuple, Any
from enum import Enum
from datetime import datetime
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.handlers.query_utilities import SELECTQueryParser
from mindsdb.integrations.utilities.sql_utils import (
    extract_comparison_conditions, 
    filter_dataframe, 
    sort_dataframe
)
from xero_python.accounting import AccountingApi


class XeroTable(APITable):
    """
    Base class for Xero API tables with common functionality
    """

    def __init__(self, handler):
        """
        Initialize the Xero table

        Args:
            handler: The Xero handler instance
        """
        super().__init__(handler)
        self.handler = handler

    def insert(self, query: ast.Insert) -> None:
        """Insert operations are not supported"""
        raise NotImplementedError("Insert operations are not supported for Xero tables")

    def update(self, query: ast.Update) -> None:
        """Update operations are not supported"""
        raise NotImplementedError("Update operations are not supported for Xero tables")

    def delete(self, query: ast.Delete) -> None:
        """Delete operations are not supported"""
        raise NotImplementedError("Delete operations are not supported for Xero tables")

    def _convert_response_to_dataframe(self, response_data: list) -> pd.DataFrame:
        """
        Convert API response to DataFrame

        Args:
            response_data: List of response objects

        Returns:
            pd.DataFrame: Flattened dataframe
        """
        if not response_data:
            return pd.DataFrame()

        # Convert objects to dictionaries
        rows = []
        for item in response_data:
            if hasattr(item, "to_dict"):
                row = item.to_dict()
            elif isinstance(item, dict):
                row = item
            else:
                row = item.__dict__
            
            # Parse objects from the model
            parsed_row = {}
            for key, value in row.items():
                if isinstance(value, Enum):
                    parsed_row.update({key: value.value})
                    continue
                if isinstance(value, dict):
                    for sub_key, sub_value in value.items():
                        if isinstance(sub_value, Enum):
                            sub_value = sub_value.value
                        parsed_row.update({f"{key}_{sub_key}": sub_value})
                    continue
                parsed_row.update({key: value})
            
            rows.append(parsed_row)

        df = pd.DataFrame(rows)
        return df

    def _map_operator_to_xero(self, sql_op: str) -> str:
        """
        Map SQL operator to Xero WHERE clause operator

        Args:
            sql_op: SQL operator (=, !=, >, <, >=, <=)

        Returns:
            str: Xero operator (== for =, others unchanged)
        """
        mapping = {
            "=": "==",
            "!=": "!=",
            ">": ">",
            "<": "<",
            ">=": ">=",
            "<=": "<=",
        }
        return mapping.get(sql_op.lower(), "==")

    def _format_value_for_xero(self, value: Any, value_type: str) -> str:
        """
        Format value for Xero WHERE clause

        Args:
            value: The value to format
            value_type: Type hint ('string', 'number', 'date', 'guid')

        Returns:
            str: Formatted value for Xero WHERE clause
        """
        if value_type == "string":
            # Escape quotes and wrap in double quotes
            escaped_value = str(value).replace('"', '\\"')
            return f'"{escaped_value}"'
        elif value_type == "number":
            return str(value)
        elif value_type == "date":
            # Convert to Xero date format: DateTime(year, month, day)
            # Handle various date formats
            if isinstance(value, datetime):
                date_obj = value
            elif isinstance(value, str):
                # Try parsing common date formats
                for fmt in ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"]:
                    try:
                        date_obj = datetime.strptime(value, fmt)
                        break
                    except ValueError:
                        continue
                else:
                    # If no format matches, try ISO format parse
                    try:
                        date_obj = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
                    except:
                        raise ValueError(f"Unable to parse date value: {value}")
            else:
                raise ValueError(f"Unsupported date type: {type(value)}")

            # Format as Xero expects: DateTime(year, month, day)
            return f'DateTime({date_obj.year}, {date_obj.month:02d}, {date_obj.day:02d})'
        elif value_type == "guid":
            return f'Guid("{value}")'
        else:
            # Default to string
            escaped_value = str(value).replace('"', '\\"')
            return f'"{escaped_value}"'

    def _parse_conditions_for_api(
        self, conditions: List, supported_filters: Dict
    ) -> Tuple[Dict, List]:
        """
        Parse WHERE conditions into API parameters and remaining conditions

        Args:
            conditions: List of [operator, column, value] from extract_comparison_conditions
            supported_filters: Dict mapping column names to their API parameter info

        Returns:
            Tuple of (api_params dict, remaining_conditions list)
        """
        api_params = {}
        remaining_conditions = []
        xero_where_clauses = []

        for op, column, value in conditions:
            # Handle BETWEEN operator by converting to >= and <=
            if op.lower() == "between":
                if not isinstance(value, (tuple, list)) or len(value) != 2:
                    remaining_conditions.append([op, column, value])
                    continue
                # Convert BETWEEN to two separate conditions: >= lower_bound AND <= upper_bound
                lower_bound, upper_bound = value
                # Recursively process the two conditions
                lower_conditions, _ = self._parse_conditions_for_api(
                    [[">=", column, lower_bound]], supported_filters
                )
                upper_conditions, _ = self._parse_conditions_for_api(
                    [["<=", column, upper_bound]], supported_filters
                )
                # Merge the conditions into api_params
                for key, val in lower_conditions.items():
                    if key == "where":
                        xero_where_clauses.append(val)
                    else:
                        api_params[key] = val
                for key, val in upper_conditions.items():
                    if key == "where":
                        xero_where_clauses.append(val)
                    else:
                        api_params[key] = val
                continue

            filter_info = supported_filters.get(column)

            if not filter_info:
                # Cannot push down, filter in memory
                remaining_conditions.append([op, column, value])
                continue

            filter_type = filter_info.get("type", "direct")

            if filter_type == "id_list":
                # For i_ds, contact_i_ds, invoice_numbers, etc.
                param_name = filter_info.get("param")
                if op == "=":
                    api_params[param_name] = [value]
                elif op == "in":
                    api_params[param_name] = value if isinstance(value, list) else [value]
                else:
                    remaining_conditions.append([op, column, value])

            elif filter_type == "where":
                # Build Xero WHERE clause
                xero_op = self._map_operator_to_xero(op)
                xero_field = filter_info.get("xero_field", column)
                value_type = filter_info.get("value_type", "string")
                xero_value = self._format_value_for_xero(value, value_type)
                xero_where_clauses.append(f"{xero_field}{xero_op}{xero_value}")

            elif filter_type == "date":
                # For date_from, date_to parameters
                param_name = filter_info.get("param")
                if op in ["=", ">=", ">"]:
                    api_params[param_name] = value
                elif op in ["<=", "<"]:
                    # Some APIs use date_to for upper bound
                    date_to_param = filter_info.get("param_upper", None)
                    if date_to_param:
                        api_params[date_to_param] = value
                    else:
                        remaining_conditions.append([op, column, value])
                else:
                    remaining_conditions.append([op, column, value])

            elif filter_type == "direct":
                # For status, contact_id, etc.
                param_name = filter_info.get("param")
                if op == "=":
                    api_params[param_name] = value
                else:
                    remaining_conditions.append([op, column, value])

        # Combine WHERE clauses with AND
        if xero_where_clauses:
            api_params["where"] = " AND ".join(xero_where_clauses)

        return api_params, remaining_conditions

    @abstractmethod
    def get_columns(self) -> List[str]:
        """Get list of available columns"""
        pass

    @abstractmethod
    def select(self, query: ast.Select) -> pd.DataFrame:
        """Execute SELECT query"""
        pass


class InvoicesTable(XeroTable):
    """Table for Xero Invoices"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "invoice_id": {"type": "id_list", "param": "ids"},
        "invoice_number": {"type": "where", "xero_field": "InvoiceNumber", "value_type": "string"},
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "total": {"type": "where", "xero_field": "Total", "value_type": "number"},
        "amount_due": {"type": "where", "xero_field": "AmountDue", "value_type": "number"},
        "contact_name": {"type": "where", "xero_field": "Contact.Name", "value_type": "string"},
        "invoice_date": {"type": "where", "xero_field": "InvoiceDate", "value_type": "date"},
        "due_date": {"type": "where", "xero_field": "DueDate", "value_type": "date"},
        "currency_code": {"type": "where", "xero_field": "CurrencyCode", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "invoice_id",
            "invoice_number",
            "reference",
            "status",
            "line_amount_types",
            "contact_name",
            "description",
            "invoice_date",
            "due_date",
            "updated_utc",
            "currency_code",
            "total",
            "amount_due",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch invoices from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        # Add pagination if limit is specified
        if query.limit and query.limit.value:
            page_size = min(query.limit.value, 100)  # Xero has limits
            api_params["page_size"] = page_size

        try:
            # Fetch invoices with optimized parameters
            invoices = api.get_invoices(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(invoices.invoices or [])
        except Exception as e:
            raise Exception(f"Failed to fetch invoices: {str(e)}")

        # Apply remaining filters in memory that couldn't be pushed to API
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query for column selection, ordering, and limiting
        parser = SELECTQueryParser(
            query, "invoices", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            # Only select requested columns
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit if not already done via pagination
        if result_limit and not query.limit:
            df = df.head(result_limit)

        return df


class ItemsTable(XeroTable):
    """Table for Xero Items"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "code": {"type": "where", "xero_field": "Code", "value_type": "string"},
        "description": {"type": "where", "xero_field": "Description", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "item_id",
            "code",
            "description",
            "inventory_asset_account_code",
            "purchase_details",
            "sales_details",
            "is_tracked_as_inventory",
            "updated_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch items from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch items with optimized parameters
            items = api.get_items(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(items.items or [])
        except Exception as e:
            raise Exception(f"Failed to fetch items: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "items", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df


class OverpaymentsTable(XeroTable):
    """Table for Xero Overpayments"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "overpayment_id",
            "contact_name",
            "type",
            "status",
            "line_amount_types",
            "updated_utc",
            "currency_code",
            "overpayment_amount",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch overpayments from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch overpayments with optimized parameters
            overpayments = api.get_overpayments(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(overpayments.overpayments or [])
        except Exception as e:
            raise Exception(f"Failed to fetch overpayments: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "overpayments", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df


class PaymentsTable(XeroTable):
    """Table for Xero Payments"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "payment_type": {"type": "where", "xero_field": "PaymentType", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "payment_id",
            "invoice_id",
            "account_code",
            "code",
            "amount",
            "payment_type",
            "status",
            "reference",
            "updated_utc",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch payments from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch payments with optimized parameters
            payments = api.get_payments(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(payments.payments or [])
        except Exception as e:
            raise Exception(f"Failed to fetch payments: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "payments", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df


class PurchaseOrdersTable(XeroTable):
    """Table for Xero Purchase Orders"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "status": {"type": "direct", "param": "status"},
        "order_date": {"type": "date", "param": "date_from", "param_upper": "date_to"},
        "delivery_date": {"type": "date", "param": "date_from", "param_upper": "date_to"},
    }

    def get_columns(self) -> List[str]:
        return [
            "purchase_order_id",
            "purchase_order_number",
            "reference",
            "status",
            "contact_name",
            "delivery_date",
            "updated_utc",
            "currency_code",
            "total",
            "order_date",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch purchase orders from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch purchase orders with optimized parameters
            purchase_orders = api.get_purchase_orders(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(purchase_orders.purchase_orders or [])
        except Exception as e:
            raise Exception(f"Failed to fetch purchase orders: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "purchase_orders", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df


class RepeatingInvoicesTable(XeroTable):
    """Table for Xero Repeating Invoices"""

    # Define which columns can be pushed to the Xero API
    SUPPORTED_FILTERS = {
        "status": {"type": "where", "xero_field": "Status", "value_type": "string"},
        "type": {"type": "where", "xero_field": "Type", "value_type": "string"},
    }

    def get_columns(self) -> List[str]:
        return [
            "repeating_invoice_id",
            "status",
            "contact_name",
            "type",
            "schedule",
            "reference",
            "updated_utc",
            "has_attachments",
        ]

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Fetch repeating invoices from Xero API

        Args:
            query: SELECT query

        Returns:
            pd.DataFrame: Query results
        """
        self.handler.connect()
        api = AccountingApi(self.handler.api_client)

        # Extract and parse WHERE conditions
        api_params = {}
        remaining_conditions = []

        if query.where:
            conditions = extract_comparison_conditions(query.where)
            api_params, remaining_conditions = self._parse_conditions_for_api(
                conditions, self.SUPPORTED_FILTERS
            )

        try:
            # Fetch repeating invoices with optimized parameters
            repeating_invoices = api.get_repeating_invoices(xero_tenant_id=self.handler.tenant_id, **api_params)
            df = self._convert_response_to_dataframe(repeating_invoices.repeating_invoices or [])
        except Exception as e:
            raise Exception(f"Failed to fetch repeating invoices: {str(e)}")

        # Apply remaining filters in memory
        if remaining_conditions and len(df) > 0:
            df = filter_dataframe(df, remaining_conditions)

        # Parse and execute query
        parser = SELECTQueryParser(
            query, "repeating_invoices", columns=self.get_columns()
        )
        selected_columns, _, order_by_conditions, result_limit = parser.parse_query()

        # Apply column selection
        if len(df) == 0:
            df = pd.DataFrame([], columns=selected_columns)
        else:
            available_columns = [col for col in selected_columns if col in df.columns]
            df = df[available_columns]

        # Apply ordering
        if order_by_conditions:
            df = sort_dataframe(df, order_by_conditions)

        # Apply limit
        if result_limit:
            df = df.head(result_limit)

        return df


import pandas as pd
import json
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
        if value_type == "bool":
            return "true" if value else "false"
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
                if op.lower() in ["in", "not in"]:
                    # Xero does not support IN directly, handle in memory
                    remaining_conditions.append([op, column, value])
                    continue
                
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
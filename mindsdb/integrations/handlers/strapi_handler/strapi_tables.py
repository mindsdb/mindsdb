from typing import List, Dict, Any, Union
import pandas as pd
from mindsdb.integrations.libs.api_handler import APIHandler, APITable
from mindsdb_sql_parser import ast
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser.ast.select.operation import BetweenOperation
from mindsdb_sql_parser.ast.select.constant import Constant
from mindsdb_sql_parser.ast.base import ASTNode
import json


def extract_or_conditions(node: ASTNode) -> list:
    """Extract conditions from a WHERE clause handling both AND and OR operations.

    Args:
        node: The AST node representing the WHERE clause

    Returns:
        List of condition groups:
        - For AND conditions: [[condition1], [condition2], ...] (each condition in separate group)
        - For OR conditions: [[condition1, condition2, ...]] (all conditions in one group)
        - Mixed: Properly nested structure
    """
    
    def extract_single_condition(node: ASTNode) -> tuple:
        if isinstance(node, ast.BinaryOperation):
            op = node.op.lower()
            arg1, arg2 = node.args
            if not isinstance(arg1, ast.Identifier):
                raise NotImplementedError(f"Not implemented arg1: {arg1}")
            if isinstance(arg2, ast.Constant):
                value = arg2.value
                return (op, arg1.parts[-1], value)
        # Add this new condition for BETWEEN
        elif isinstance(node, BetweenOperation):
            field = node.args[0]  # The field being tested
            min_val = node.args[1]  # Lower bound
            max_val = node.args[2]  # Upper bound
            
            if isinstance(field, ast.Identifier) and isinstance(min_val, ast.Constant) and isinstance(max_val, ast.Constant):
                return ('between', field.parts[-1], [min_val.value, max_val.value])
            else:
                raise NotImplementedError(f"BETWEEN with non-constant values not supported")

        raise NotImplementedError(f"Unsupported condition type: {type(node)}")
    
    def extract_conditions_recursive(node: ASTNode) -> list:
        if isinstance(node, ast.BinaryOperation):
            if node.op.lower() == 'or':
                # For OR operations, combine conditions into one group
                left_conditions = extract_conditions_recursive(node.args[0])
                right_conditions = extract_conditions_recursive(node.args[1])
                
                # Flatten all conditions into a single group for OR
                combined_conditions = []
                for group in left_conditions:
                    if isinstance(group, list):
                        combined_conditions.extend(group)
                    else:
                        combined_conditions.append(group)
                for group in right_conditions:
                    if isinstance(group, list):
                        combined_conditions.extend(group)
                    else:
                        combined_conditions.append(group)
                
                return [combined_conditions]  # Single group with all OR conditions
                
            elif node.op.lower() == 'and':
                # For AND operations, keep conditions in separate groups
                left_conditions = extract_conditions_recursive(node.args[0])
                right_conditions = extract_conditions_recursive(node.args[1])
                
                # Combine the groups (AND means separate groups)
                return left_conditions + right_conditions
                
            else:
                # For comparison operations, return as single condition in a group
                condition = extract_single_condition(node)
                return [[condition]]  # Single condition in its own group

        elif isinstance(node, BetweenOperation):
                condition = extract_single_condition(node)
                return [[condition]]  # Single condition in its own group
        
        raise NotImplementedError(f"Unsupported node type: {type(node)}")

    try:
        conditions = extract_conditions_recursive(node)
        return conditions
    except Exception as e:
        return [[]]


# Mapping SQL operators to Strapi filter operators
OPERATOR_MAP = {
    '=': '$eq',
    '!=': '$ne',
    '>': '$gt',
    '>=': '$gte',
    '<': '$lt',
    '<=': '$lte',
    'IN': '$in',
    'NOT IN': '$notIn'
}

class StrapiTable(APITable):
    def __init__(self, handler: APIHandler, name: str, defer_schema_fetch: bool = False):
        super().__init__(handler)
        self.name = name
        self._schema_fetched = False

        if not defer_schema_fetch:
            self._fetch_schema()
        else:
            # Set basic Strapi columns as placeholder
            self.columns = ["id", "documentId", "createdAt", "updatedAt"]

    def _fetch_schema(self):
        """Fetch schema from Strapi API"""
        if self._schema_fetched:
            return

        # Use cached schema if available
        schema_key = f"{self.handler._connection_key}_{self.name}"
        if schema_key in self.handler._table_schemas:
            self.columns = self.handler._table_schemas[schema_key]
            self._schema_fetched = True
            return

        # Only fetch schema once and cache it
        try:
            df = self.handler.call_strapi_api(
                method="GET", endpoint=f"/api/{self.name}", params={"pagination[limit]": 1}
            )
            if len(df.columns) > 0:
                self.columns = df.columns.tolist()
                self.handler._table_schemas[schema_key] = self.columns
            else:
                # If no data, set basic Strapi columns
                self.columns = ["id", "documentId", "createdAt", "updatedAt"]
                self.handler._table_schemas[schema_key] = self.columns
        except Exception as e:
            # Set basic Strapi columns as fallback
            self.columns = ["id", "documentId", "createdAt", "updatedAt"]
            self.handler._table_schemas[schema_key] = self.columns

        self._schema_fetched = True

    def _build_filters(self, conditions: List[List[tuple]]) -> Dict[str, Any]:
        """Build Strapi filters from SQL conditions
        
        Args:
            conditions: List of condition groups from WHERE clause
                    - Each group represents conditions that should be ORed together
                    - Different groups are ANDed together
        
        Returns:
            Dict of Strapi filter parameters
        """
        filters = {}
        or_groups = []
        and_conditions = {}

        for condition_group in conditions:
            if not isinstance(condition_group, list):
                continue
                
            # If group has multiple conditions, it's an OR group
            if len(condition_group) > 1:
                or_group_conditions = []
                for condition in condition_group:
                    if isinstance(condition, tuple) and len(condition) == 3:
                        op, field, value = condition

                        # Handle special case for documentId
                        if field == 'documentId' and op == '=':
                            return {'documentId': value}
                        
                        condition_dict = self._build_single_condition(op, field, value)
                        or_group_conditions.append(condition_dict)
                
                or_groups.append(or_group_conditions)
                
            # If group has single condition, it's an AND condition
            elif len(condition_group) == 1:
                condition = condition_group[0]
                if isinstance(condition, tuple) and len(condition) == 3:
                    op, field, value = condition

                    # Handle special case for documentId
                    if field == 'documentId' and op == '=':
                        return {'documentId': value}
                    
                    condition_dict = self._build_single_condition(op, field, value)
                    # Add to AND conditions (these will be separate filter parameters)
                    for field_name, field_filters in condition_dict.items():
                        for op_key, field_value in field_filters.items():
                            filters[f'filters[{field_name}][{op_key}]'] = field_value
        
        # Handle OR groups
        if or_groups:
            if len(or_groups) == 1 and len(and_conditions) == 0:
                # Single OR group, no AND conditions
                or_conditions = or_groups[0]
                for idx, condition in enumerate(or_conditions):
                    for field, field_filters in condition.items():
                        for op_key, value in field_filters.items():
                            filters[f'filters[$or][{idx}][{field}][{op_key}]'] = value
            else:
                # Multiple OR groups or mixed with AND - more complex case
                # This would require more sophisticated handling
                # For now, handle the first OR group
                if or_groups:
                    or_conditions = or_groups[0]
                    for idx, condition in enumerate(or_conditions):
                        for field, field_filters in condition.items():
                            for op_key, value in field_filters.items():
                                filters[f'filters[$or][{idx}][{field}][{op_key}]'] = value
        
        return filters

    def _build_single_condition(self, op: str, field: str, value: Any) -> Dict[str, Dict[str, Any]]:
        """Build a single condition dictionary for Strapi filters
        
        Args:
            op: SQL operator
            field: Field name
            value: Field value
            
        Returns:
            Dictionary with field and its filter conditions
        """
        condition = {}
        
        if op.upper() == 'BETWEEN':
            if isinstance(value, (list, tuple)) and len(value) == 2:
                # BETWEEN translates to field >= min AND field <= max
                condition[field] = {
                    '$gte': value[0],
                    '$lte': value[1]
                }
            else:
                raise ValueError("BETWEEN operator requires exactly 2 values")
        
        elif op.upper() == 'LIKE':
            if not isinstance(value, str):
                raise ValueError("LIKE operator requires a string value")
            
            # Remove quotes if present
            if (value.startswith("'") and value.endswith("'")) or \
            (value.startswith('"') and value.endswith('"')):
                value = value[1:-1]
            
            # Handle LIKE patterns
            if value.startswith('%') and value.endswith('%'):
                value = value[1:-1]  # Remove % from both ends
                condition[field] = {'$contains': value}
            elif value.startswith('%'):
                value = value[1:]  # Remove leading %
                condition[field] = {'$endsWith': value}
            elif value.endswith('%'):
                value = value[:-1]  # Remove trailing %
                condition[field] = {'$startsWith': value}
            else:
                condition[field] = {'$eq': value}
        
        elif op.upper() == 'IS':
            if value is None:
                condition[field] = {'$null': True}
            else:
                raise ValueError(f"IS operator with non-null value not supported: {value}")
        
        elif op.upper() == 'IS NOT':
            if value is None:
                condition[field] = {'$notNull': True}
            else:
                raise ValueError(f"IS NOT operator with non-null value not supported: {value}")
        
        elif op.upper() in ('IN', 'NOT IN'):
            if isinstance(value, (list, tuple)):
                strapi_op = '$in' if op.upper() == 'IN' else '$notIn'
                condition[field] = {strapi_op: list(value)}
            else:
                raise ValueError(f"{op} operator requires a list or tuple value")
        
        elif op.upper() in OPERATOR_MAP:
            condition[field] = {OPERATOR_MAP[op.upper()]: value}
        
        else:
            raise ValueError(f"Unsupported operator {op} in WHERE clause")
        
        return condition

    def _fetch_by_id(self, document_id: str, selected_columns: list) -> pd.DataFrame:
        """Helper method to fetch a record by documentId

        Args:
            document_id (str): The documentId to fetch
            selected_columns (list): Columns to include in result

        Returns:
            pd.DataFrame: The resulting DataFrame
        """
        df = self.handler.call_strapi_api(
            method='GET', endpoint=f'/api/{self.name}/{document_id}')

        if len(df) > 0:
            return df[selected_columns]
        return pd.DataFrame(columns=selected_columns)

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Triggered at the SELECT query

        Args:
            query (ast.Select): User's entered query

        Returns:
            pd.DataFrame: The queried information
        """
        # Get selected columns from query
        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        # Default to all columns if no columns are selected
        if not selected_columns:
            selected_columns = self.get_columns()

        # Build filters from WHERE clause
        filters = {}
        if query.where:
            try:
                # Extract OR conditions - now always returns list of lists
                conditions = extract_or_conditions(query.where)
                filters = self._build_filters(conditions)
            except Exception as e:
                # Fallback to empty filters
                filters = {}

        # If we got a documentId filter, use the specific endpoint
        if 'documentId' in filters:
            return self._fetch_by_id(filters['documentId'], selected_columns)
        
        # Initialize pagination parameters with optimized page size
        # Use Strapi's default maximum page size of 100 for REST API
        page_size = 100
        limit = query.limit.value if query.limit else None
        result_df = pd.DataFrame(columns=selected_columns)
        
        # If limit is specified and smaller than page_size, use limit as page_size to minimize API calls
        if limit and limit < page_size:
            page_size = limit

        # Prepare initial parameters including filters
        params = {
            'pagination[page]': 1,
            'pagination[pageSize]': page_size,
            **filters  # Add any WHERE clause filters
        }

        page = 1
        total_fetched = 0
        
        # Fetch data in optimized pagination loop
        while True:
            params['pagination[page]'] = page
            
            df = self.handler.call_strapi_api(
                method='GET',
                endpoint=f'/api/{self.name}',
                params=params
            )

            # Break if no data returned
            if len(df) == 0:
                break
            
            # Apply limit constraint if specified
            rows_to_take = len(df)
            if limit:
                remaining_needed = limit - total_fetched
                if remaining_needed <= 0:
                    break
                rows_to_take = min(rows_to_take, remaining_needed)
            
            # Take only the needed rows and add to result
            df_slice = df.head(rows_to_take) if rows_to_take < len(df) else df
            result_df = pd.concat([result_df, df_slice[selected_columns]], ignore_index=True)
            
            total_fetched += rows_to_take
            
            # Break conditions:
            # 1. If we got fewer rows than page_size, we've reached the end
            # 2. If we have a limit and we've reached it
            # 3. If we took fewer rows than available due to limit constraint
            if len(df) < page_size or (limit and total_fetched >= limit) or rows_to_take < len(df):
                break
                
            page += 1

        return result_df

    def insert(self, query: ast.Insert) -> None:
        """triggered at the INSERT query
        Args:
            query (ast.Insert): user's entered query
        """
        # Loop through all rows in the VALUES clause
        for row_values in query.values:
            data = {'data': {}}

            for column, value in zip(query.columns, row_values):
                # Clean column name (remove backticks if present)
                column_name = column.name
                if column_name.startswith('`') and column_name.endswith('`'):
                    column_name = column_name[1:-1]
                    
                if isinstance(value, Constant):
                    data['data'][column_name] = value.value
                else:
                    data['data'][column_name] = value
            
            # Make individual API call for each row
            self.handler.call_strapi_api(
                method='POST', 
                endpoint=f'/api/{self.name}', 
                json_data=json.dumps(data)
            )

    def update(self, query: ast.Update) -> None:
        """triggered at the UPDATE query

        Args:
            query (ast.Update): user's entered query
        """
        conditions = extract_comparison_conditions(query.where)
        # Get documentId from query
        for op, arg1, arg2 in conditions:
            if arg1 == "documentId" and op == '=':
                _documentId = arg2
            else:
                raise ValueError("`documentId` must be used in WHERE clause for UPDATE")
        data = {'data': {}}
        for key, value in query.update_columns.items():
            if isinstance(value, Constant):
                data['data'][key] = value.value
        self.handler.call_strapi_api(method='PUT', endpoint=f'/api/{self.name}/{_documentId}', json_data=json.dumps(data))

    def get_columns(self, ignore: List[str] = []) -> List[str]:
        """columns

        Args:
            ignore (List[str], optional): exclusion items. Defaults to [].

        Returns:
            List[str]: available columns with `ignore` items removed from the list.
        """
        if not self._schema_fetched:
            self._fetch_schema()
        return [item for item in self.columns if item not in ignore]

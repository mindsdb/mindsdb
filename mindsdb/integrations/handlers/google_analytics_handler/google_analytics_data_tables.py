"""
Google Analytics Data API Tables

This module implements table classes for the Google Analytics Data API (GA4),
providing access to reporting data through SQL-like queries.
"""

import pandas as pd
from typing import List

from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    RunRealtimeReportRequest,
    GetMetadataRequest,
    FilterExpression,
    FilterExpressionList,
    Filter,
    OrderBy,
)
from google.api_core.exceptions import GoogleAPIError

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast
from mindsdb.utilities import log

logger = log.getLogger(__name__)


def _collect_identifiers(node) -> List[str]:
    """Recursively collect all Identifier column names from an AST node.

    Walks into CASE WHEN, Function args, BinaryOperation, etc. so that
    columns referenced inside complex expressions are not missed.
    """
    if node is None:
        return []
    if isinstance(node, ast.Identifier):
        return [str(node.parts[-1])]
    if isinstance(node, ast.Case):
        names = []
        for condition, result in node.rules:
            names.extend(_collect_identifiers(condition))
            names.extend(_collect_identifiers(result))
        names.extend(_collect_identifiers(node.default))
        return names
    if isinstance(node, ast.Function):
        names = []
        for arg in (node.args or []):
            names.extend(_collect_identifiers(arg))
        return names
    if isinstance(node, ast.BinaryOperation):
        return _collect_identifiers(node.args[0]) + _collect_identifiers(node.args[1])
    if isinstance(node, ast.UnaryOperation):
        return _collect_identifiers(node.args[0])
    if isinstance(node, ast.TypeCast):
        return _collect_identifiers(node.arg)
    return []


class ReportsTable(APITable):
    """
    Table for running standard Google Analytics reports using the Data API.

    Supports the runReport method which returns customized reports of GA4 event data.

    Example SQL queries:
        SELECT country, city, activeUsers, sessions
        FROM reports
        WHERE start_date = '30daysAgo'
          AND end_date = 'today'
        ORDER BY activeUsers DESC
        LIMIT 100;

        SELECT date, eventName, eventCount
        FROM reports
        WHERE start_date = '2024-01-01'
          AND end_date = '2024-01-31'
          AND dimension_eventName = 'first_open';
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Execute a SELECT query on the reports table.

        Args:
            query: SQL SELECT query AST

        Returns:
            pandas DataFrame containing the report data
        """
        try:
            # Extract conditions from WHERE clause.
            # ignore_functions=True unwraps LOWER(col)/UPPER(col) so callers can
            # write WHERE LOWER(sessionSourceMedium) = 'google / organic'.
            conditions = extract_comparison_conditions(query.where, ignore_functions=True) if query.where else []

            # Extract required date range and filters
            start_date = '30daysAgo'
            end_date = 'today'
            dimension_filters = {}
            metric_filters = {}

            for op, arg, val in conditions:
                if arg == 'start_date':
                    start_date = val
                elif arg == 'end_date':
                    end_date = val
                elif arg.startswith('dimension_'):
                    dimension_name = arg.replace('dimension_', '')
                    # Convert back to API format (underscore to colon)
                    api_dimension_name = self._unsanitize_column_name(dimension_name)
                    dimension_filters[api_dimension_name] = (op, val)
                elif arg.startswith('metric_'):
                    metric_name = arg.replace('metric_', '')
                    # Convert back to API format (underscore to colon)
                    api_metric_name = self._unsanitize_column_name(metric_name)
                    metric_filters[api_metric_name] = (op, val)
                else:
                    # Column name without prefix - auto-detect if it's a dimension or metric
                    api_name = self._unsanitize_column_name(arg)
                    if self._is_metric(arg):
                        metric_filters[api_name] = (op, val)
                    else:
                        dimension_filters[api_name] = (op, val)

            # Extract dimensions and metrics from SELECT columns.
            # Use _collect_identifiers to recurse into CASE WHEN, SUM(...), and other
            # complex expressions so that all referenced columns are fetched from GA4.
            dimensions = []
            metrics = []

            if query.targets:
                has_star = any(isinstance(t, ast.Star) for t in query.targets)
                if has_star:
                    dimensions = [Dimension(name='date'), Dimension(name='country')]
                    metrics = [Metric(name='activeUsers'), Metric(name='sessions')]
                else:
                    seen = set()
                    for target in query.targets:
                        for col_name in _collect_identifiers(target):
                            if col_name in seen:
                                continue
                            seen.add(col_name)
                            api_name = self._unsanitize_column_name(col_name)
                            if self._is_metric(col_name):
                                metrics.append(Metric(name=api_name))
                            else:
                                dimensions.append(Dimension(name=api_name))

            # If no dimensions/metrics specified, use defaults
            if not dimensions:
                dimensions = [Dimension(name='date')]
            if not metrics:
                metrics = [Metric(name='activeUsers')]

            # Build date range
            date_ranges = [DateRange(start_date=start_date, end_date=end_date)]

            # Build dimension filters
            dimension_filter = self._build_dimension_filter(dimension_filters) if dimension_filters else None

            # Build metric filters
            metric_filter = self._build_metric_filter(metric_filters) if metric_filters else None

            # Build order by
            order_bys = self._build_order_by(query.order_by) if query.order_by else []

            # Extract limit and offset
            limit = query.limit.value if query.limit else 10000
            offset = query.offset.value if query.offset else 0

            # Build the request
            request = RunReportRequest(
                property=f"properties/{self.handler.property_id}",
                dimensions=dimensions,
                metrics=metrics,
                date_ranges=date_ranges,
                limit=limit,
                offset=offset,
            )

            # Add filters if present
            if dimension_filter:
                request.dimension_filter = dimension_filter
            if metric_filter:
                request.metric_filter = metric_filter
            if order_bys:
                request.order_bys = order_bys

            logger.info(
                "[TEMP DEBUG][google_analytics] run_report integration=%s handler_property_id=%s request_property=%s dimensions=%s metrics=%s "
                "start_date=%s end_date=%s limit=%s offset=%s",
                getattr(self.handler, "name", None),
                self.handler.property_id,
                request.property,
                [d.name for d in dimensions],
                [m.name for m in metrics],
                start_date,
                end_date,
                limit,
                offset,
            )

            # Execute the request
            self.handler.connect()
            response = self.handler.data_service.run_report(request)
            logger.info(
                "[TEMP DEBUG][google_analytics] run_report_response integration=%s request_property=%s row_count=%s "
                "time_zone=%s currency_code=%s",
                getattr(self.handler, "name", None),
                request.property,
                getattr(response, "row_count", None),
                getattr(response.metadata, "time_zone", None) if hasattr(response, "metadata") else None,
                getattr(response.metadata, "currency_code", None) if hasattr(response, "metadata") else None,
            )

            # Convert response to DataFrame
            df = self._response_to_dataframe(response)

            return df

        except GoogleAPIError as e:
            logger.error(f"Google Analytics Data API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error running report: {e}")
            raise

    def _is_metric(self, name: str) -> bool:
        """
        Determine if a column name is a metric (vs dimension) using metadata lookup.
        Falls back to heuristic if metadata is unavailable.

        Args:
            name: Column name (can be sanitized with underscores or API format with colons)

        Returns:
            bool: True if field is a metric, False if dimension
        """
        # Get cached metadata from handler
        metadata_cache = self.handler.get_metadata_cache()

        # Check if we have metadata available
        if metadata_cache and (metadata_cache['metrics'] or metadata_cache['dimensions']):
            # Check both original name and API name (with colon restored)
            api_name = self._unsanitize_column_name(name)

            # Definitive check: if in metrics set, it's a metric
            if name in metadata_cache['metrics'] or api_name in metadata_cache['metrics']:
                return True

            # Definitive check: if in dimensions set, it's a dimension
            if name in metadata_cache['dimensions'] or api_name in metadata_cache['dimensions']:
                return False

        # Fallback to heuristic if metadata unavailable or field not found
        logger.debug(f"Falling back to heuristic for field: {name}")
        metric_keywords = [
            'users', 'sessions', 'views', 'count', 'rate', 'revenue', 'value',
            'duration', 'conversions', 'events', 'transactions', 'purchases',
            'bounces', 'engagement', 'scrolls', 'clicks', 'cost', 'impressions',
            'advertiser', 'publisher', 'adsense', 'adx', 'cm360', 'dv360', 'sa360',
            'total', 'average', 'avg', 'sum', 'per', 'quantity', 'amount'
        ]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in metric_keywords)

    def _unsanitize_column_name(self, column_name: str) -> str:
        """
        Convert sanitized column name back to GA4 API format.
        Uses metadata cache for accurate mapping, with fallback to heuristic.

        Examples:
            keyEvents_click_job -> keyEvents:click_job (via cache lookup)
            customEvent_job_title -> customEvent:job_title (via cache lookup)
            customUser_subscription_tier -> customUser:subscription_tier (via cache lookup)
            country -> country (no change for standard fields)
            unknown_field -> unknown_field (fallback for uncached fields)
        """
        # Try cache lookup first (most accurate)
        metadata_cache = self.handler.get_metadata_cache()
        if metadata_cache and 'column_to_api' in metadata_cache:
            column_to_api = metadata_cache['column_to_api']
            if column_name in column_to_api:
                return column_to_api[column_name]

        # Fallback to heuristic for backward compatibility
        # (handles cases where cache is unavailable or field not in cache)
        if column_name.startswith('customEvent_'):
            return column_name.replace('customEvent_', 'customEvent:', 1)
        elif column_name.startswith('customUser_'):
            return column_name.replace('customUser_', 'customUser:', 1)
        elif column_name.startswith('keyEvents_'):
            return column_name.replace('keyEvents_', 'keyEvents:', 1)
        elif column_name.startswith('sessionKeyEventRate_'):
            return column_name.replace('sessionKeyEventRate_', 'sessionKeyEventRate:', 1)
        else:
            # Standard dimension/metric, return as-is
            return column_name

    def _build_dimension_filter(self, dimension_filters: dict) -> FilterExpression:
        """Build dimension filter from dimension filters dictionary

        Args:
            dimension_filters: Dict mapping field names to (operator, value) tuples

        Returns:
            FilterExpression for GA4 API
        """
        if not dimension_filters:
            return None

        filters = []
        for field_name, filter_spec in dimension_filters.items():
            # Handle both old format (value only) and new format ((op, value))
            if isinstance(filter_spec, tuple):
                op, value = filter_spec
            else:
                # Backward compatibility: if just a value, assume equals
                op, value = ('=', filter_spec)

            # Check if this is a NOT operation
            is_negated = False
            if op.upper().startswith('NOT '):
                is_negated = True
                op = op[4:].strip()  # Remove 'NOT ' prefix

            # Map SQL operators to GA4 StringFilter match types
            if op.upper() == 'LIKE':
                # Convert SQL LIKE pattern to GA4 CONTAINS
                # Remove SQL wildcards (% at start/end)
                pattern = str(value).strip('%')
                if value.startswith('%') and value.endswith('%'):
                    # %pattern% -> CONTAINS
                    match_type = Filter.StringFilter.MatchType.CONTAINS
                elif value.startswith('%'):
                    # %pattern -> ENDS_WITH
                    match_type = Filter.StringFilter.MatchType.ENDS_WITH
                elif value.endswith('%'):
                    # pattern% -> BEGINS_WITH
                    match_type = Filter.StringFilter.MatchType.BEGINS_WITH
                else:
                    # pattern -> EXACT (no wildcards)
                    match_type = Filter.StringFilter.MatchType.EXACT
                    pattern = value

                filter_expr = FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(
                            value=pattern,
                            match_type=match_type
                        ),
                    )
                )
            else:
                # For =, !=, etc., use EXACT match
                match_type = Filter.StringFilter.MatchType.EXACT
                filter_expr = FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(
                            value=str(value),
                            match_type=match_type
                        ),
                    )
                )

            # Wrap in NOT expression if needed
            if is_negated:
                filter_expr = FilterExpression(
                    not_expression=filter_expr
                )

            filters.append(filter_expr)

        if len(filters) == 1:
            return filters[0]

        # Multiple filters - use AND logic
        return FilterExpression(
            and_group=FilterExpressionList(expressions=filters)
        )

    def _build_metric_filter(self, metric_filters: dict) -> FilterExpression:
        """Build metric filter from metric filters dictionary"""
        if not metric_filters:
            return None

        filters = []
        for field_name, (op, value) in metric_filters.items():
            # Map SQL operators to GA4 filter operations
            operation = Filter.NumericFilter.Operation.EQUAL
            if op == '>':
                operation = Filter.NumericFilter.Operation.GREATER_THAN
            elif op == '>=':
                operation = Filter.NumericFilter.Operation.GREATER_THAN_OR_EQUAL
            elif op == '<':
                operation = Filter.NumericFilter.Operation.LESS_THAN
            elif op == '<=':
                operation = Filter.NumericFilter.Operation.LESS_THAN_OR_EQUAL
            elif op == '=':
                operation = Filter.NumericFilter.Operation.EQUAL

            filters.append(
                FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        numeric_filter=Filter.NumericFilter(
                            operation=operation,
                            value={'double_value': float(value)}
                        ),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpressionList(expressions=filters)
        )

    def _build_order_by(self, order_by_clause) -> List[OrderBy]:
        """Build OrderBy objects from SQL ORDER BY clause"""
        order_bys = []

        if not order_by_clause:
            return order_bys

        for order in order_by_clause:
            field_name = order.field.parts[-1] if hasattr(order.field, 'parts') else str(order.field)
            # Convert back to API format (underscore to colon)
            api_field_name = self._unsanitize_column_name(field_name)
            desc = order.direction.upper() == 'DESC' if hasattr(order, 'direction') and order.direction else False

            # Determine if ordering by metric or dimension
            if self._is_metric(field_name):
                order_bys.append(
                    OrderBy(
                        metric=OrderBy.MetricOrderBy(metric_name=api_field_name),
                        desc=desc
                    )
                )
            else:
                order_bys.append(
                    OrderBy(
                        dimension=OrderBy.DimensionOrderBy(dimension_name=api_field_name),
                        desc=desc
                    )
                )

        return order_bys

    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """Convert GA4 API response to pandas DataFrame"""

        # Extract column names and sanitize them (replace colons with underscores)
        # This handles custom dimensions like customEvent:job_title -> customEvent_job_title
        columns = []
        for header in response.dimension_headers:
            sanitized_name = header.name.replace(':', '_')
            columns.append(sanitized_name)
        for header in response.metric_headers:
            sanitized_name = header.name.replace(':', '_')
            columns.append(sanitized_name)

        # Extract data rows
        data = []
        for row in response.rows:
            row_data = []
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            for metric_value in row.metric_values:
                # Convert metric values to float for proper sorting and arithmetic operations
                try:
                    row_data.append(float(metric_value.value))
                except (ValueError, TypeError):
                    # If conversion fails, keep as string
                    row_data.append(metric_value.value)
            data.append(row_data)

        return pd.DataFrame(data, columns=columns)

    def get_columns(self) -> List[str]:
        """Return list of available columns (this is dynamic based on query)"""
        return ['*']  # Dynamic schema

    def list(self):
        """
        Dummy list method to prevent parent APIHandler from overriding query.targets.
        This method is never actually called - select() is used instead.
        """
        raise NotImplementedError("Use select() method instead")


class RealtimeReportsTable(APITable):
    """
    Table for running realtime Google Analytics reports.

    Supports the runRealtimeReport method which returns realtime event data
    (last 30-60 minutes).

    Example SQL queries:
        SELECT country, activeUsers
        FROM realtime_reports;

        SELECT city, screenPageViews
        FROM realtime_reports
        WHERE dimension_country = 'US';
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Execute a SELECT query on the realtime_reports table.

        Args:
            query: SQL SELECT query AST

        Returns:
            pandas DataFrame containing the realtime report data
        """
        try:
            # Extract conditions from WHERE clause.
            # ignore_functions=True unwraps LOWER(col)/UPPER(col) so callers can
            # write WHERE LOWER(sessionSourceMedium) = 'google / organic'.
            conditions = extract_comparison_conditions(query.where, ignore_functions=True) if query.where else []

            # Extract dimension filters
            dimension_filters = {}
            metric_filters = {}

            for op, arg, val in conditions:
                if arg.startswith('dimension_'):
                    dimension_name = arg.replace('dimension_', '')
                    # Convert back to API format (underscore to colon)
                    api_dimension_name = self._unsanitize_column_name(dimension_name)
                    dimension_filters[api_dimension_name] = (op, val)
                elif arg.startswith('metric_'):
                    metric_name = arg.replace('metric_', '')
                    # Convert back to API format (underscore to colon)
                    api_metric_name = self._unsanitize_column_name(metric_name)
                    metric_filters[api_metric_name] = (op, val)
                else:
                    # Column name without prefix - auto-detect if it's a dimension or metric
                    api_name = self._unsanitize_column_name(arg)
                    if self._is_metric(arg):
                        metric_filters[api_name] = (op, val)
                    else:
                        dimension_filters[api_name] = (op, val)

            # Extract dimensions and metrics from SELECT columns
            dimensions = []
            metrics = []

            if query.targets:
                for target in query.targets:
                    if isinstance(target, ast.Star):
                        # Default realtime dimensions and metrics
                        dimensions = [Dimension(name='country')]
                        metrics = [Metric(name='activeUsers')]
                        break
                    elif isinstance(target, ast.Identifier):
                        col_name = str(target.parts[-1])
                        # Convert underscores back to colons for custom dimensions (reverse sanitization)
                        api_name = self._unsanitize_column_name(col_name)
                        if self._is_metric(col_name):
                            metrics.append(Metric(name=api_name))
                        else:
                            dimensions.append(Dimension(name=api_name))

            if not dimensions:
                dimensions = [Dimension(name='country')]
            if not metrics:
                metrics = [Metric(name='activeUsers')]

            # Build dimension filters
            dimension_filter = self._build_dimension_filter(dimension_filters) if dimension_filters else None

            # Build metric filters
            metric_filter = self._build_metric_filter(metric_filters) if metric_filters else None

            # Extract limit
            limit = query.limit.value if query.limit else 10000

            # Build the request
            request = RunRealtimeReportRequest(
                property=f"properties/{self.handler.property_id}",
                dimensions=dimensions,
                metrics=metrics,
                limit=limit,
            )

            # Add filters if present
            if dimension_filter:
                request.dimension_filter = dimension_filter
            if metric_filter:
                request.metric_filter = metric_filter

            logger.info(
                "[TEMP DEBUG][google_analytics] run_realtime_report integration=%s handler_property_id=%s request_property=%s dimensions=%s "
                "metrics=%s limit=%s",
                getattr(self.handler, "name", None),
                self.handler.property_id,
                request.property,
                [d.name for d in dimensions],
                [m.name for m in metrics],
                limit,
            )

            # Execute the request
            self.handler.connect()
            response = self.handler.data_service.run_realtime_report(request)

            # Convert response to DataFrame
            df = self._response_to_dataframe(response)

            return df

        except GoogleAPIError as e:
            logger.error(f"Google Analytics Data API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error running realtime report: {e}")
            raise

    def _is_metric(self, name: str) -> bool:
        """
        Determine if a column name is a metric (vs dimension) using metadata lookup.
        Falls back to heuristic if metadata is unavailable.

        Args:
            name: Column name (can be sanitized with underscores or API format with colons)

        Returns:
            bool: True if field is a metric, False if dimension
        """
        # Get cached metadata from handler
        metadata_cache = self.handler.get_metadata_cache()

        # Check if we have metadata available
        if metadata_cache and (metadata_cache['metrics'] or metadata_cache['dimensions']):
            # Check both original name and API name (with colon restored)
            api_name = self._unsanitize_column_name(name)

            # Definitive check: if in metrics set, it's a metric
            if name in metadata_cache['metrics'] or api_name in metadata_cache['metrics']:
                return True

            # Definitive check: if in dimensions set, it's a dimension
            if name in metadata_cache['dimensions'] or api_name in metadata_cache['dimensions']:
                return False

        # Fallback to heuristic if metadata unavailable or field not found
        logger.debug(f"Falling back to heuristic for field: {name}")
        metric_keywords = [
            'users', 'sessions', 'views', 'count', 'rate', 'revenue', 'value',
            'duration', 'conversions', 'events', 'transactions', 'purchases',
            'bounces', 'engagement', 'scrolls', 'clicks', 'cost', 'impressions',
            'advertiser', 'publisher', 'adsense', 'adx', 'cm360', 'dv360', 'sa360',
            'total', 'average', 'avg', 'sum', 'per', 'quantity', 'amount'
        ]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in metric_keywords)

    def _unsanitize_column_name(self, column_name: str) -> str:
        """
        Convert sanitized column name back to GA4 API format.
        Uses metadata cache for accurate mapping, with fallback to heuristic.

        Examples:
            keyEvents_click_job -> keyEvents:click_job (via cache lookup)
            customEvent_job_title -> customEvent:job_title (via cache lookup)
            customUser_subscription_tier -> customUser:subscription_tier (via cache lookup)
            country -> country (no change for standard fields)
            unknown_field -> unknown_field (fallback for uncached fields)
        """
        # Try cache lookup first (most accurate)
        metadata_cache = self.handler.get_metadata_cache()
        if metadata_cache and 'column_to_api' in metadata_cache:
            column_to_api = metadata_cache['column_to_api']
            if column_name in column_to_api:
                return column_to_api[column_name]

        # Fallback to heuristic for backward compatibility
        # (handles cases where cache is unavailable or field not in cache)
        if column_name.startswith('customEvent_'):
            return column_name.replace('customEvent_', 'customEvent:', 1)
        elif column_name.startswith('customUser_'):
            return column_name.replace('customUser_', 'customUser:', 1)
        elif column_name.startswith('keyEvents_'):
            return column_name.replace('keyEvents_', 'keyEvents:', 1)
        elif column_name.startswith('sessionKeyEventRate_'):
            return column_name.replace('sessionKeyEventRate_', 'sessionKeyEventRate:', 1)
        else:
            return column_name

    def _build_dimension_filter(self, dimension_filters: dict) -> FilterExpression:
        """Build dimension filter from dimension filters dictionary

        Args:
            dimension_filters: Dict mapping field names to (operator, value) tuples

        Returns:
            FilterExpression for GA4 API
        """
        if not dimension_filters:
            return None

        filters = []
        for field_name, filter_spec in dimension_filters.items():
            # Handle both old format (value only) and new format ((op, value))
            if isinstance(filter_spec, tuple):
                op, value = filter_spec
            else:
                # Backward compatibility: if just a value, assume equals
                op, value = ('=', filter_spec)

            # Check if this is a NOT operation
            is_negated = False
            if op.upper().startswith('NOT '):
                is_negated = True
                op = op[4:].strip()  # Remove 'NOT ' prefix

            # Map SQL operators to GA4 StringFilter match types
            if op.upper() == 'LIKE':
                # Convert SQL LIKE pattern to GA4 CONTAINS
                # Remove SQL wildcards (% at start/end)
                pattern = str(value).strip('%')
                if value.startswith('%') and value.endswith('%'):
                    # %pattern% -> CONTAINS
                    match_type = Filter.StringFilter.MatchType.CONTAINS
                elif value.startswith('%'):
                    # %pattern -> ENDS_WITH
                    match_type = Filter.StringFilter.MatchType.ENDS_WITH
                elif value.endswith('%'):
                    # pattern% -> BEGINS_WITH
                    match_type = Filter.StringFilter.MatchType.BEGINS_WITH
                else:
                    # pattern -> EXACT (no wildcards)
                    match_type = Filter.StringFilter.MatchType.EXACT
                    pattern = value

                filter_expr = FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(
                            value=pattern,
                            match_type=match_type
                        ),
                    )
                )
            else:
                # For =, !=, etc., use EXACT match
                match_type = Filter.StringFilter.MatchType.EXACT
                filter_expr = FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(
                            value=str(value),
                            match_type=match_type
                        ),
                    )
                )

            # Wrap in NOT expression if needed
            if is_negated:
                filter_expr = FilterExpression(
                    not_expression=filter_expr
                )

            filters.append(filter_expr)

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpressionList(expressions=filters)
        )

    def _build_metric_filter(self, metric_filters: dict) -> FilterExpression:
        """Build metric filter from metric filters dictionary"""
        if not metric_filters:
            return None

        filters = []
        for field_name, (op, value) in metric_filters.items():
            operation = Filter.NumericFilter.Operation.EQUAL
            if op == '>':
                operation = Filter.NumericFilter.Operation.GREATER_THAN
            elif op == '>=':
                operation = Filter.NumericFilter.Operation.GREATER_THAN_OR_EQUAL
            elif op == '<':
                operation = Filter.NumericFilter.Operation.LESS_THAN
            elif op == '<=':
                operation = Filter.NumericFilter.Operation.LESS_THAN_OR_EQUAL
            elif op == '=':
                operation = Filter.NumericFilter.Operation.EQUAL

            filters.append(
                FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        numeric_filter=Filter.NumericFilter(
                            operation=operation,
                            value={'double_value': float(value)}
                        ),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpressionList(expressions=filters)
        )

    def _response_to_dataframe(self, response) -> pd.DataFrame:
        """Convert GA4 API response to pandas DataFrame"""
        if response.row_count == 0:
            return pd.DataFrame()

        # Extract column names and sanitize them (replace colons with underscores)
        # This handles custom dimensions like customEvent:job_title -> customEvent_job_title
        columns = []
        for header in response.dimension_headers:
            sanitized_name = header.name.replace(':', '_')
            columns.append(sanitized_name)
        for header in response.metric_headers:
            sanitized_name = header.name.replace(':', '_')
            columns.append(sanitized_name)

        data = []
        for row in response.rows:
            row_data = []
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            for metric_value in row.metric_values:
                # Convert metric values to float for proper sorting and arithmetic operations
                try:
                    row_data.append(float(metric_value.value))
                except (ValueError, TypeError):
                    # If conversion fails, keep as string
                    row_data.append(metric_value.value)
            data.append(row_data)

        return pd.DataFrame(data, columns=columns)

    def get_columns(self) -> List[str]:
        """Return list of available columns"""
        return ['*']

    def list(self):
        """
        Dummy list method to prevent parent APIHandler from overriding query.targets.
        This method is never actually called - select() is used instead.
        """
        raise NotImplementedError("Use select() method instead")


class MetadataTable(APITable):
    """
    Table for fetching available dimensions and metrics metadata.

    Supports the getMetadata method which returns information about
    available dimensions and metrics for the property, including custom dimensions.

    The table includes both api_name (original GA4 API name with colons) and
    column_name (sanitized name with underscores for SQL usage).

    Example SQL queries:
        -- Get all dimensions and metrics
        SELECT * FROM metadata;

        -- See custom dimensions with both API and SQL column names
        SELECT api_name, column_name, ui_name FROM metadata WHERE custom = true;

        -- Filter by type
        SELECT * FROM metadata WHERE type = 'dimension';
    """

    def select(self, query: ast.Select) -> pd.DataFrame:
        """
        Execute a SELECT query on the metadata table.

        Args:
            query: SQL SELECT query AST

        Returns:
            pandas DataFrame containing the metadata
        """
        try:
            # Extract conditions from WHERE clause.
            # ignore_functions=True unwraps LOWER(col)/UPPER(col) so callers can
            # write WHERE LOWER(sessionSourceMedium) = 'google / organic'.
            conditions = extract_comparison_conditions(query.where, ignore_functions=True) if query.where else []

            # Extract filter type if specified
            filter_type = None
            for op, arg, val in conditions:
                if arg == 'type':
                    filter_type = val.lower()  # 'dimension' or 'metric'
                    break

            # Build the request
            request = GetMetadataRequest(
                name=f"properties/{self.handler.property_id}/metadata"
            )

            # Execute the request
            self.handler.connect()
            response = self.handler.data_service.get_metadata(request)

            # Convert to DataFrame
            data = []

            # Add dimensions
            if not filter_type or filter_type == 'dimension':
                for dimension in response.dimensions:
                    # Sanitize column name (replace colons with underscores)
                    column_name = dimension.api_name.replace(':', '_')
                    data.append({
                        'type': 'dimension',
                        'api_name': dimension.api_name,
                        'column_name': column_name,
                        'ui_name': dimension.ui_name,
                        'description': dimension.description,
                        'custom': dimension.custom_definition,
                        'deprecated': bool(dimension.deprecated_api_names),
                        'category': dimension.category if hasattr(dimension, 'category') else None,
                    })

            # Add metrics
            if not filter_type or filter_type == 'metric':
                for metric in response.metrics:
                    # Sanitize column name (replace colons with underscores)
                    column_name = metric.api_name.replace(':', '_')
                    data.append({
                        'type': 'metric',
                        'api_name': metric.api_name,
                        'column_name': column_name,
                        'ui_name': metric.ui_name,
                        'description': metric.description,
                        'custom': metric.custom_definition,
                        'deprecated': bool(metric.deprecated_api_names),
                        'category': metric.category if hasattr(metric, 'category') else None,
                        'metric_type': str(metric.type_) if hasattr(metric, 'type_') else None,
                    })

            df = pd.DataFrame(data)

            return df

        except GoogleAPIError as e:
            logger.error(f"Google Analytics Data API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching metadata: {e}")
            raise

    def get_columns(self) -> List[str]:
        """Return list of columns in metadata table"""
        return ['type', 'api_name', 'column_name', 'ui_name', 'description', 'custom', 'deprecated', 'category', 'metric_type']

    def list(self):
        """
        Dummy list method to prevent parent APIHandler from overriding query.targets.
        This method is never actually called - select() is used instead.
        """
        raise NotImplementedError("Use select() method instead")

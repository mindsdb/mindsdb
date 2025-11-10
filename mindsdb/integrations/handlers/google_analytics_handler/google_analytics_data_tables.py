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
    Filter,
    OrderBy,
)
from google.api_core.exceptions import GoogleAPIError

from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb_sql_parser import ast
from mindsdb.utilities import log

logger = log.getLogger(__name__)


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
            # DEBUG: Log what we receive
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"DEBUG ReportsTable.select() - query.targets: {query.targets}")
            logger.error(f"DEBUG ReportsTable.select() - query.targets types: {[type(t).__name__ for t in query.targets]}")
            if query.targets:
                for i, target in enumerate(query.targets):
                    if isinstance(target, ast.Identifier):
                        logger.error(f"DEBUG Target {i}: Identifier - parts={target.parts}, alias={target.alias}")
                    elif isinstance(target, ast.Star):
                        logger.error(f"DEBUG Target {i}: Star")
                    else:
                        logger.error(f"DEBUG Target {i}: {type(target).__name__}")

            # Extract conditions from WHERE clause
            conditions = extract_comparison_conditions(query.where) if query.where else []

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
                    dimension_filters[api_dimension_name] = val
                elif arg.startswith('metric_'):
                    metric_name = arg.replace('metric_', '')
                    # Convert back to API format (underscore to colon)
                    api_metric_name = self._unsanitize_column_name(metric_name)
                    metric_filters[api_metric_name] = (op, val)

            # Extract dimensions and metrics from SELECT columns
            dimensions = []
            metrics = []

            if query.targets:
                for target in query.targets:
                    if isinstance(target, ast.Star):
                        # If SELECT *, use default dimensions and metrics
                        dimensions = [Dimension(name='date'), Dimension(name='country')]
                        metrics = [Metric(name='activeUsers'), Metric(name='sessions')]
                        break
                    elif isinstance(target, ast.Identifier):
                        col_name = str(target.parts[-1])
                        # Convert underscores back to colons for custom dimensions (reverse sanitization)
                        # customEvent_job_title -> customEvent:job_title
                        api_name = self._unsanitize_column_name(col_name)
                        # Determine if it's a dimension or metric based on common patterns
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

            # Execute the request
            self.handler.connect()
            response = self.handler.data_service.run_report(request)

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
        Determine if a column name is likely a metric (vs dimension).
        Common metric patterns: contains 'Users', 'Count', 'Rate', 'Revenue', 'Sessions', etc.
        """
        metric_keywords = [
            'users', 'sessions', 'views', 'count', 'rate', 'revenue', 'value',
            'duration', 'conversions', 'events', 'transactions', 'purchases',
            'bounces', 'engagement', 'scrolls', 'clicks'
        ]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in metric_keywords)

    def _unsanitize_column_name(self, column_name: str) -> str:
        """
        Convert sanitized column name back to GA4 API format.
        Converts underscores back to colons for custom dimensions/metrics.

        Examples:
            customEvent_job_title -> customEvent:job_title
            customUser_subscription_tier -> customUser:subscription_tier
            country -> country (no change for standard fields)
        """
        # Only convert if it starts with customEvent_ or customUser_
        if column_name.startswith('customEvent_'):
            return column_name.replace('customEvent_', 'customEvent:', 1)
        elif column_name.startswith('customUser_'):
            return column_name.replace('customUser_', 'customUser:', 1)
        else:
            # Standard dimension/metric, return as-is
            return column_name

    def _build_dimension_filter(self, dimension_filters: dict) -> FilterExpression:
        """Build dimension filter from dimension filters dictionary"""
        if not dimension_filters:
            return None

        filters = []
        for field_name, value in dimension_filters.items():
            filters.append(
                FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(value=str(value)),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        # Multiple filters - use AND logic
        return FilterExpression(
            and_group=FilterExpression.FilterExpressionList(expressions=filters)
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
                            value=Filter.NumericFilter.NumericValue(
                                double_value=float(value)
                            )
                        ),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpression.FilterExpressionList(expressions=filters)
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

        # Extract data rows
        data = []
        for row in response.rows:
            row_data = []
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            for metric_value in row.metric_values:
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
            # Extract conditions from WHERE clause
            conditions = extract_comparison_conditions(query.where) if query.where else []

            # Extract dimension filters
            dimension_filters = {}
            metric_filters = {}

            for op, arg, val in conditions:
                if arg.startswith('dimension_'):
                    dimension_name = arg.replace('dimension_', '')
                    # Convert back to API format (underscore to colon)
                    api_dimension_name = self._unsanitize_column_name(dimension_name)
                    dimension_filters[api_dimension_name] = val
                elif arg.startswith('metric_'):
                    metric_name = arg.replace('metric_', '')
                    # Convert back to API format (underscore to colon)
                    api_metric_name = self._unsanitize_column_name(metric_name)
                    metric_filters[api_metric_name] = (op, val)

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
        """Determine if a column name is likely a metric"""
        metric_keywords = [
            'users', 'sessions', 'views', 'count', 'rate', 'revenue', 'value',
            'duration', 'conversions', 'events', 'transactions', 'purchases',
            'bounces', 'engagement', 'scrolls', 'clicks'
        ]
        name_lower = name.lower()
        return any(keyword in name_lower for keyword in metric_keywords)

    def _unsanitize_column_name(self, column_name: str) -> str:
        """
        Convert sanitized column name back to GA4 API format.
        Converts underscores back to colons for custom dimensions/metrics.
        """
        if column_name.startswith('customEvent_'):
            return column_name.replace('customEvent_', 'customEvent:', 1)
        elif column_name.startswith('customUser_'):
            return column_name.replace('customUser_', 'customUser:', 1)
        else:
            return column_name

    def _build_dimension_filter(self, dimension_filters: dict) -> FilterExpression:
        """Build dimension filter from dimension filters dictionary"""
        if not dimension_filters:
            return None

        filters = []
        for field_name, value in dimension_filters.items():
            filters.append(
                FilterExpression(
                    filter=Filter(
                        field_name=field_name,
                        string_filter=Filter.StringFilter(value=str(value)),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpression.FilterExpressionList(expressions=filters)
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
                            value=Filter.NumericFilter.NumericValue(
                                double_value=float(value)
                            )
                        ),
                    )
                )
            )

        if len(filters) == 1:
            return filters[0]

        return FilterExpression(
            and_group=FilterExpression.FilterExpressionList(expressions=filters)
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
            # Extract conditions from WHERE clause
            conditions = extract_comparison_conditions(query.where) if query.where else []

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

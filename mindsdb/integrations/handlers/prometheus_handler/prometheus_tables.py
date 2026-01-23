from typing import List, Dict, Text, Any, Optional, Tuple
import pandas as pd
from datetime import datetime
import json

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log
from mindsdb_sql_parser.ast import (
    BinaryOperation,
    Function,
    Identifier,
    Constant,
    Select,
    BetweenOperation,
)

logger = log.getLogger(__name__)


class MetricsTable(APIResource):
    """Prometheus Metrics table - lists all available metrics with metadata."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        """Return static metadata for the metrics table."""
        return {
            "TABLE_NAME": "metrics",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "List of all metrics available in Prometheus with metadata (name, type, help, unit)",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Return column metadata for metrics."""
        return [
            {
                "TABLE_NAME": "metrics",
                "COLUMN_NAME": "metric_name",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Name of the metric",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metrics",
                "COLUMN_NAME": "type",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Metric type (counter, gauge, histogram, summary, etc.)",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metrics",
                "COLUMN_NAME": "help",
                "DATA_TYPE": "TEXT",
                "COLUMN_DESCRIPTION": "Help text describing the metric",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metrics",
                "COLUMN_NAME": "unit",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Unit of measurement for the metric",
                "IS_NULLABLE": True,
            },
        ]

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """List all metrics with their metadata."""
        try:
            # Get all metric names
            response = self.handler.call_prometheus_api("/api/v1/label/__name__/values")
            metric_names = response.get("data", [])

            # Get metadata for all metrics
            metadata_response = self.handler.call_prometheus_api("/api/v1/metadata")
            metadata = metadata_response.get("data", {})

            metrics_data = []
            for metric_name in metric_names:
                metric_info = {
                    "metric_name": metric_name,
                    "type": None,
                    "help": None,
                    "unit": None,
                }

                # Extract metadata if available
                if metric_name in metadata:
                    metric_metadata = metadata[metric_name]
                    if metric_metadata:
                        # Get the first entry (most recent)
                        first_entry = (
                            metric_metadata[0]
                            if isinstance(metric_metadata, list)
                            else metric_metadata
                        )
                        metric_info["type"] = first_entry.get("type")
                        metric_info["help"] = first_entry.get("help")
                        metric_info["unit"] = first_entry.get("unit")

                metrics_data.append(metric_info)

            df = pd.DataFrame(metrics_data)

            # Apply filters if any
            if conditions:
                for condition in conditions:
                    if condition.column == "metric_name" and condition.op.value == "=":
                        df = df[df["metric_name"] == condition.value]
                    elif (
                        condition.column == "metric_name" and condition.op.value == "!="
                    ):
                        df = df[df["metric_name"] != condition.value]

            if limit:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error listing metrics: {str(e)}")
            return pd.DataFrame(columns=["metric_name", "type", "help", "unit"])

    def get_columns(self) -> List[Text]:
        """Return column names for the metrics table."""
        return ["metric_name", "type", "help", "unit"]


class LabelsTable(APIResource):
    """Prometheus Labels table - lists all labels available for metrics."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        """Return static metadata for the labels table."""
        return {
            "TABLE_NAME": "labels",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "List of all labels available for metrics with common label values",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Return column metadata for labels."""
        return [
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "metric_name",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Name of the metric",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "label",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "JSON string of all label names",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "job",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Job label value",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "instance",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Instance label value",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "method",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Method label value",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "status",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Status label value",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "labels",
                "COLUMN_NAME": "custom_labels",
                "DATA_TYPE": "TEXT",
                "COLUMN_DESCRIPTION": "JSON string of custom label key-value pairs",
                "IS_NULLABLE": True,
            },
        ]

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """List all labels with their values."""
        try:
            # Get all label names
            response = self.handler.call_prometheus_api("/api/v1/labels")
            # Label names
            _ = response.get("data", [])

            # Get all metric names
            metric_response = self.handler.call_prometheus_api(
                "/api/v1/label/__name__/values"
            )
            metric_names = metric_response.get("data", [])

            labels_data = []
            # Sample a subset of metrics to avoid too much data
            sample_metrics = (
                metric_names[:100] if len(metric_names) > 100 else metric_names
            )

            for metric_name in sample_metrics:
                # Query a sample of the metric to get label combinations
                query = f'{{__name__="{metric_name}"}}'
                query_response = self.handler.call_prometheus_api(
                    "/api/v1/query", params={"query": query}
                )

                if query_response.get("status") == "success":
                    results = query_response.get("data", {}).get("result", [])
                    for result in results[:10]:  # Limit to 10 samples per metric
                        labels = result.get("metric", {})
                        custom_labels = {
                            k: v
                            for k, v in labels.items()
                            if k
                            not in ["__name__", "job", "instance", "method", "status"]
                        }

                        labels_data.append(
                            {
                                "metric_name": metric_name,
                                "label": json.dumps(list(labels.keys())),
                                "job": labels.get("job"),
                                "instance": labels.get("instance"),
                                "method": labels.get("method"),
                                "status": labels.get("status"),
                                "custom_labels": (
                                    json.dumps(custom_labels) if custom_labels else None
                                ),
                            }
                        )

            df = pd.DataFrame(labels_data)

            # Apply filters
            if conditions:
                for condition in conditions:
                    col = condition.column
                    op = condition.op.value
                    val = condition.value
                    if col == "metric_name" and op == "=":
                        df = df[df["metric_name"] == val]
                    elif col == "metric_name" and op == "!=":
                        df = df[df["metric_name"] != val]

            if limit:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error listing labels: {str(e)}")
            return pd.DataFrame(
                columns=[
                    "metric_name",
                    "label",
                    "job",
                    "instance",
                    "method",
                    "status",
                    "custom_labels",
                ]
            )

    def get_columns(self) -> List[Text]:
        """Return column names for the labels table."""
        return [
            "metric_name",
            "label",
            "job",
            "instance",
            "method",
            "status",
            "custom_labels",
        ]


class ScrapeTargetsTable(APIResource):
    """Prometheus Scrape Targets table - lists all scrape targets."""

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        """Return static metadata for the scrape_targets table."""
        return {
            "TABLE_NAME": "scrape_targets",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "List of all scrape targets with their health status and URLs",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Return column metadata for scrape_targets."""
        return [
            {
                "TABLE_NAME": "scrape_targets",
                "COLUMN_NAME": "target_labels",
                "DATA_TYPE": "TEXT",
                "COLUMN_DESCRIPTION": "JSON string of target label key-value pairs",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "scrape_targets",
                "COLUMN_NAME": "health",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Health status of the target (up, down, unknown)",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "scrape_targets",
                "COLUMN_NAME": "scrape_url",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "URL from which the target is scraped",
                "IS_NULLABLE": True,
            },
        ]

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """List all scrape targets."""
        try:
            response = self.handler.call_prometheus_api("/api/v1/targets")
            targets_data = response.get("data", {}).get("activeTargets", [])

            scrape_targets = []
            for target in targets_data:
                # Prometheus API returns labels as a flat dict, not a list
                # Example: {"job": "prometheus", "instance": "localhost:9090"}
                labels = target.get("labels", {})

                # Handle both dict format (standard) and list format (legacy)
                if isinstance(labels, list):
                    # Legacy format: list of {name, value} dicts
                    labels_dict = {
                        label.get("name"): label.get("value")
                        for label in labels
                        if label.get("name") and label.get("value")
                    }
                elif isinstance(labels, dict):
                    # Standard format: flat dict
                    labels_dict = labels
                else:
                    labels_dict = {}

                scrape_targets.append(
                    {
                        "target_labels": json.dumps(labels_dict),
                        "health": target.get("health"),
                        "scrape_url": target.get("scrapeUrl"),
                    }
                )

            df = pd.DataFrame(scrape_targets)

            # Apply filters
            if conditions:
                for condition in conditions:
                    col = condition.column
                    op = condition.op.value
                    val = condition.value
                    if col == "health" and op == "=":
                        df = df[df["health"] == val]

            if limit:
                df = df.head(limit)

            return df

        except Exception as e:
            logger.error(f"Error listing scrape targets: {str(e)}")
            return pd.DataFrame(columns=["target_labels", "health", "scrape_url"])

    def get_columns(self) -> List[Text]:
        """Return column names for the scrape_targets table."""
        return ["target_labels", "health", "scrape_url"]


def _parse_json_path_expression(node: Any) -> Optional[Tuple[str, str]]:
    """Parse JSON path expression to extract label name.

    Supports:
    - PostgreSQL style: labels_json->>'label_name'
    - MySQL style: JSON_EXTRACT(labels_json, '$.label_name')

    Args:
        node: AST node (BinaryOperation for ->>, Function for JSON_EXTRACT)

    Returns:
        Tuple of (label_name, json_path_type) or None if not a JSON path expression
    """
    # PostgreSQL style: labels_json->>'label_name'
    if isinstance(node, BinaryOperation) and node.op in ("->>", "->"):
        left = node.args[0]
        right = node.args[1]

        # Check if left side is labels_json column
        if isinstance(left, Identifier) and left.parts[-1] == "labels_json":
            if isinstance(right, Constant):
                label_name = right.value
                # Remove quotes if present
                if isinstance(label_name, str):
                    label_name = label_name.strip("'\"")
                return (label_name, "postgres")

    # MySQL style: JSON_EXTRACT(labels_json, '$.label_name')
    if isinstance(node, Function) and node.op.upper() == "JSON_EXTRACT":
        if len(node.args) >= 2:
            first_arg = node.args[0]
            second_arg = node.args[1]

            # Check if first arg is labels_json
            if (
                isinstance(first_arg, Identifier)
                and first_arg.parts[-1] == "labels_json"
            ):
                if isinstance(second_arg, Constant):
                    json_path = second_arg.value
                    # Extract label name from JSON path like '$.label_name'
                    if isinstance(json_path, str) and json_path.startswith("$."):
                        label_name = json_path[2:].strip("'\"")
                        return (label_name, "mysql")

    return None


def _extract_json_path_filters(
    raw_conditions: List[Any],
) -> Tuple[Dict[str, Dict[str, Any]], List[Any]]:
    """Extract label filters from JSON path expressions in raw_conditions.

    Args:
        raw_conditions: List of raw condition AST nodes

    Returns:
        Tuple of (label_filters_dict, remaining_conditions)
    """
    label_filters = {}
    remaining_conditions = []

    for condition in raw_conditions:
        if isinstance(condition, BinaryOperation):
            # Check if left side is a JSON path expression
            left = condition.args[0]
            right = condition.args[1]
            op = condition.op.upper()

            json_path_info = _parse_json_path_expression(left)
            if json_path_info:
                label_name, _ = json_path_info

                # Extract value from right side
                if isinstance(right, Constant):
                    value = right.value
                    label_filters[label_name] = {"op": op, "value": value}
                else:
                    # Complex right side, keep as remaining
                    remaining_conditions.append(condition)
            else:
                # Not a JSON path expression, keep as remaining
                remaining_conditions.append(condition)
        elif isinstance(condition, BetweenOperation):
            # Handle BETWEEN operation
            left = condition.args[0]
            lower = condition.args[1]
            upper = condition.args[2]

            json_path_info = _parse_json_path_expression(left)
            if json_path_info:
                label_name, _ = json_path_info

                if isinstance(lower, Constant) and isinstance(upper, Constant):
                    label_filters[label_name] = {
                        "op": "BETWEEN",
                        "value": (lower.value, upper.value),
                    }
                else:
                    remaining_conditions.append(condition)
            else:
                remaining_conditions.append(condition)
        else:
            # Unknown condition type, keep as remaining
            remaining_conditions.append(condition)

    return label_filters, remaining_conditions


class MetricDataTable(APIResource):
    """Prometheus Metric Data table - query metric data with PromQL.

    Supports:
    - Instant queries (single timestamp)
    - Range queries (time series with step)
    - JSON path label filtering (PostgreSQL and MySQL syntax)
    - INSERT operations to Pushgateway
    """

    def meta_get_tables(self, table_name: str) -> Dict[str, Any]:
        """Return static metadata for the metric_data table."""
        return {
            "TABLE_NAME": "metric_data",
            "TABLE_TYPE": "BASE TABLE",
            "TABLE_DESCRIPTION": "Query metric data using PromQL with label filtering support",
            "ROW_COUNT": None,
        }

    def meta_get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Return column metadata for metric_data."""
        return [
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "pql_query",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "PromQL query string (required)",
                "IS_NULLABLE": False,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "start_ts",
                "DATA_TYPE": "TIMESTAMP",
                "COLUMN_DESCRIPTION": "Start timestamp for range queries (ISO format or Unix timestamp)",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "end_ts",
                "DATA_TYPE": "TIMESTAMP",
                "COLUMN_DESCRIPTION": "End timestamp for range queries (ISO format or Unix timestamp)",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "step",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Query resolution step width (e.g., '10s', '1m', '5m')",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "timeout",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "Query timeout (e.g., '10s')",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "timestamp",
                "DATA_TYPE": "TIMESTAMP",
                "COLUMN_DESCRIPTION": "Timestamp of the data point",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "value",
                "DATA_TYPE": "DOUBLE",
                "COLUMN_DESCRIPTION": "Metric value",
                "IS_NULLABLE": True,
            },
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "labels_json",
                "DATA_TYPE": "JSON",
                "COLUMN_DESCRIPTION": "JSON object of all labels and their values for this row",
                "IS_NULLABLE": True,
            },
        ]

    def select(self, query: Select) -> pd.DataFrame:
        """Override select with a fallback for complex JSON syntax."""
        from mindsdb.integrations.utilities.sql_utils import (
            FilterCondition,
            FilterOperator,
        )
        from mindsdb_sql_parser.ast import (
            BinaryOperation,
            Identifier,
            Constant,
            BetweenOperation,
        )

        try:
            # Try to use the built-in parser first
            api_conditions, raw_conditions = self._extract_conditions(
                query.where, strict=False
            )

            # Extract JSON path filters from raw_conditions
            json_path_filters, remaining_raw_conditions = _extract_json_path_filters(
                raw_conditions
            )

        except Exception:
            # If _extract_conditions fails (e.g. precedence issues with ->>), we parse manually
            api_conditions = []
            json_path_filters = {}
            remaining_raw_conditions = []

            # 1. Find pql_query recursively (even if buried in JSON logic)
            def find_pql(node):
                if isinstance(node, BinaryOperation) and node.op == "=":
                    left, right = node.args[0], node.args[1]
                    if isinstance(left, Identifier) and left.parts[-1] == "pql_query":
                        if isinstance(right, Constant):
                            return right.value
                if hasattr(node, "args"):
                    for arg in node.args:
                        res = find_pql(arg)
                        if res:
                            return res
                return None

            pql_val = find_pql(query.where)
            if pql_val:
                api_conditions.append(
                    FilterCondition(
                        column="pql_query", op=FilterOperator.EQUAL, value=pql_val
                    )
                )

            # 2. Flatten and find JSON filters
            nodes = []

            def flatten(node):
                if isinstance(node, BinaryOperation) and node.op.upper() == "AND":
                    flatten(node.args[0])
                    flatten(node.args[1])
                else:
                    nodes.append(node)

            if query.where:
                flatten(query.where)

            for node in nodes:
                target = None
                op = None
                val = None

                # Identify comparison vs BETWEEN
                if isinstance(node, BetweenOperation):
                    target, op = node.args[0], "BETWEEN"
                    if isinstance(node.args[1], Constant):
                        val = (node.args[1].value, node.args[2].value)
                elif isinstance(node, BinaryOperation) and node.op.upper() not in [
                    "AND",
                    "OR",
                ]:
                    target, op = node.args[0], node.op.upper()
                    if isinstance(node.args[1], Constant):
                        val = node.args[1].value

                # Extract Key if target is ->>
                if (
                    target
                    and isinstance(target, BinaryOperation)
                    and target.op == "->>"
                ):
                    if isinstance(target.args[1], Constant):
                        json_path_filters[target.args[1].value] = {
                            "op": op,
                            "value": val,
                        }

        limit = None
        if query.limit:
            limit = query.limit.value

        sort = None
        if query.order_by and len(query.order_by) > 0:
            from mindsdb.integrations.utilities.sql_utils import SortColumn

            sort = []
            for an_order in query.order_by:
                if isinstance(an_order.field, Identifier):
                    sort.append(
                        SortColumn(
                            an_order.field.parts[-1],
                            an_order.direction.upper() != "DESC",
                        )
                    )

        targets = []
        for col in query.targets:
            if isinstance(col, Identifier):
                targets.append(col.parts[-1])

        # Call list with JSON path filters
        result = self.list(
            conditions=api_conditions,
            json_path_filters=json_path_filters,
            limit=limit,
            sort=sort,
            targets=targets,
        )

        # Apply remaining raw conditions using filter_dataframe
        if remaining_raw_conditions:
            from mindsdb.integrations.utilities.sql_utils import filter_dataframe

            filters = []
            result = filter_dataframe(
                result, filters, raw_conditions=remaining_raw_conditions
            )

        if limit is not None and len(result) > limit:
            result = result[: int(limit)]

        query.where = None

        return result

    def list(
        self,
        conditions: List[FilterCondition] = None,
        json_path_filters: Dict[str, Dict[str, Any]] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Query metric data using PromQL.

        Args:
            conditions: List of FilterCondition objects for query parameters
            json_path_filters: Dictionary of label filters from JSON path expressions
            limit: Maximum number of rows to return
            sort: List of SortColumn objects for ordering
            targets: List of column names to select

        Returns:
            DataFrame with query results
        """
        try:
            # Extract query parameters from conditions
            pql_query = None
            start_ts = None
            end_ts = None
            step = None
            timeout = None
            label_filters = json_path_filters or {}

            if conditions:
                for condition in conditions:
                    col = condition.column
                    op = condition.op.value
                    val = condition.value

                    if col == "pql_query":
                        if op == "=":
                            pql_query = val
                    elif col == "start_ts":
                        if op == "=":
                            start_ts = val
                    elif col == "end_ts":
                        if op == "=":
                            end_ts = val
                    elif col == "step":
                        if op == "=":
                            step = val
                    elif col == "timeout":
                        if op == "=":
                            timeout = val
                    # Note: Direct label column filtering is deprecated in favor of JSON path syntax

            if not pql_query:
                raise ValueError("pql_query parameter is required")

            # Build the PromQL query with label filters
            # Returns (final_query, filters_not_pushed_down)
            final_query, filters_not_pushed = self._build_query_with_label_filters(
                pql_query, label_filters
            )

            # Prepare API parameters
            params = {"query": final_query}
            if timeout:
                params["timeout"] = timeout

            # Determine if this is a range query or instant query
            if start_ts and end_ts and step:
                # Range query
                params["start"] = self._parse_timestamp(start_ts)
                params["end"] = self._parse_timestamp(end_ts)
                params["step"] = step

                response = self.handler.call_prometheus_api(
                    "/api/v1/query_range", params=params
                )
                return self._process_range_query_response(
                    response,
                    pql_query,
                    start_ts,
                    end_ts,
                    step,
                    timeout,
                    filters_not_pushed,
                )
            else:
                # Instant query
                if start_ts:
                    params["time"] = self._parse_timestamp(start_ts)

                response = self.handler.call_prometheus_api(
                    "/api/v1/query", params=params
                )
                return self._process_instant_query_response(
                    response,
                    pql_query,
                    start_ts,
                    end_ts,
                    step,
                    timeout,
                    filters_not_pushed,
                )

        except Exception as e:
            logger.error(f"Error querying metric data: {str(e)}")
            raise

    def _build_query_with_label_filters(
        self, base_query: str, label_filters: Dict[str, Dict[str, Any]]
    ) -> Tuple[str, Dict[str, Dict[str, Any]]]:
        """Build PromQL query with label filters applied.

        Only simple equality (=) and inequality (!=) filters can be pushed down
        to PromQL. Complex operators (>, <, >=, <=, BETWEEN) must be applied
        in-memory after the query returns.

        Args:
            base_query: Base PromQL query
            label_filters: Dictionary of label filters {label_name: {op: operator, value: value}}

        Returns:
            Tuple of (modified PromQL query, filters_not_pushed_down)
        """
        if not label_filters:
            return base_query, {}

        filters_not_pushed = {}
        label_parts = []

        # Check if the query already has label selectors
        has_existing_selectors = "{" in base_query and "}" in base_query

        for label_name, filter_info in label_filters.items():
            op = filter_info["op"]
            val = filter_info["value"]

            # PromQL only supports: =, !=, =~, !~ for label matching
            # All other operators must be handled in-memory
            if op == "=":
                label_parts.append(f'{label_name}="{val}"')
            elif op == "!=":
                label_parts.append(f'{label_name}!="{val}"')
            else:
                # Complex operators (>, <, >=, <=, BETWEEN) cannot be pushed to PromQL
                # These must be applied in-memory after query returns
                filters_not_pushed[label_name] = filter_info

        # Add label filters to query if we have any to push down
        if label_parts:
            if has_existing_selectors:
                # Try to merge with existing selectors
                # Find the closing brace and insert before it
                last_brace = base_query.rfind("}")
                if last_brace != -1:
                    # Insert before closing brace
                    base_query = (
                        base_query[:last_brace]
                        + ", "
                        + ", ".join(label_parts)
                        + base_query[last_brace:]
                    )
                else:
                    # Fallback: wrap entire query
                    base_query = f"{base_query}{{{', '.join(label_parts)}}}"
            else:
                # Add braces with label filters
                base_query = f"{base_query}{{{', '.join(label_parts)}}}"

        return base_query, filters_not_pushed

    def _parse_timestamp(self, ts: Any) -> str:
        """Parse timestamp to Prometheus format (Unix timestamp or RFC3339).

        Args:
            ts: Timestamp as string (ISO format) or number (Unix timestamp)

        Returns:
            String representation of timestamp

        Raises:
            ValueError: If timestamp format is invalid
        """
        if isinstance(ts, (int, float)):
            return str(ts)
        elif isinstance(ts, str):
            # Try to parse as ISO format
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return str(dt.timestamp())
            except ValueError:
                # Try as Unix timestamp string
                try:
                    float(ts)
                    return ts
                except ValueError:
                    raise ValueError(f"Invalid timestamp format: {ts}")
        else:
            raise ValueError(f"Invalid timestamp type: {type(ts)}")

    def _apply_label_filters(
        self, labels_dict: Dict[str, Any], label_filters: Dict[str, Dict[str, Any]]
    ) -> bool:
        """Apply label filters to labels dictionary.

        Args:
            labels_dict: Dictionary of label key-value pairs
            label_filters: Dictionary of filters {label_name: {op: operator, value: value}}

        Returns:
            True if row should be included, False otherwise
        """
        for label_name, filter_info in label_filters.items():
            label_value = labels_dict.get(label_name)
            if label_value is None:
                return False

            op = filter_info["op"]
            val = filter_info["value"]

            if op == "=":
                if label_value != val:
                    return False
            elif op == "!=":
                if label_value == val:
                    return False
            elif op == "BETWEEN":
                if isinstance(val, tuple) and len(val) == 2:
                    lower, upper = val
                    try:
                        label_num = float(label_value)
                        lower_num = float(lower)
                        upper_num = float(upper)
                        if not (lower_num <= label_num <= upper_num):
                            return False
                    except (ValueError, TypeError):
                        # String comparison for BETWEEN
                        if not (str(lower) <= str(label_value) <= str(upper)):
                            return False
                else:
                    return False
            elif op in (">", "<", ">=", "<="):
                # Try numeric comparison
                try:
                    label_num = float(label_value)
                    val_num = float(val)
                    if op == ">" and not (label_num > val_num):
                        return False
                    elif op == "<" and not (label_num < val_num):
                        return False
                    elif op == ">=" and not (label_num >= val_num):
                        return False
                    elif op == "<=" and not (label_num <= val_num):
                        return False
                except (ValueError, TypeError):
                    # String comparison fallback
                    logger.warning(
                        f"Label value '{label_value}' or filter value '{val}' is not numeric, using string comparison"
                    )
                    if op == ">" and not (str(label_value) > str(val)):
                        return False
                    elif op == "<" and not (str(label_value) < str(val)):
                        return False
                    elif op == ">=" and not (str(label_value) >= str(val)):
                        return False
                    elif op == "<=" and not (str(label_value) <= str(val)):
                        return False

        return True

    def _process_instant_query_response(
        self,
        response: Dict[str, Any],
        pql_query: str,
        start_ts: Any,
        end_ts: Any,
        step: Any,
        timeout: Any,
        label_filters: Dict,
    ) -> pd.DataFrame:
        """Process instant query response into DataFrame."""
        if response.get("status") != "success":
            raise Exception(
                f"Prometheus query failed: {response.get('error', 'Unknown error')}"
            )

        results = response.get("data", {}).get("result", [])
        rows = []

        for result in results:
            metric = result.get("metric", {})
            value = result.get("value", [None, None])
            timestamp = value[0] if len(value) > 0 else None
            metric_value = value[1] if len(value) > 1 else None

            # Build labels JSON (excluding __name__)
            labels_dict = {k: v for k, v in metric.items() if k != "__name__"}
            labels_json = json.dumps(labels_dict) if labels_dict else None

            # Apply label filters in memory if not applied in query
            if label_filters:
                should_include = self._apply_label_filters(labels_dict, label_filters)
                if not should_include:
                    continue

            row = {
                "pql_query": pql_query,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "step": step,
                "timeout": timeout,
                "timestamp": timestamp,
                "value": metric_value,
                "labels_json": labels_json,
            }

            rows.append(row)

        if not rows:
            return pd.DataFrame(columns=self.get_columns())

        df = pd.DataFrame(rows)
        return df

    def _process_range_query_response(
        self,
        response: Dict[str, Any],
        pql_query: str,
        start_ts: Any,
        end_ts: Any,
        step: Any,
        timeout: Any,
        label_filters: Dict,
    ) -> pd.DataFrame:
        """Process range query response into DataFrame."""
        if response.get("status") != "success":
            raise Exception(
                f"Prometheus query failed: {response.get('error', 'Unknown error')}"
            )

        results = response.get("data", {}).get("result", [])
        rows = []

        for result in results:
            metric = result.get("metric", {})
            values = result.get("values", [])

            # Build labels JSON (excluding __name__)
            labels_dict = {k: v for k, v in metric.items() if k != "__name__"}
            labels_json = json.dumps(labels_dict) if labels_dict else None

            # Apply label filters in memory if not applied in query
            if label_filters:
                should_include = self._apply_label_filters(labels_dict, label_filters)
                if not should_include:
                    continue

            # Create a row for each timestamp-value pair
            for value_pair in values:
                timestamp = value_pair[0] if len(value_pair) > 0 else None
                metric_value = value_pair[1] if len(value_pair) > 1 else None

                row = {
                    "pql_query": pql_query,
                    "start_ts": start_ts,
                    "end_ts": end_ts,
                    "step": step,
                    "timeout": timeout,
                    "timestamp": timestamp,
                    "value": metric_value,
                    "labels_json": labels_json,
                }

                rows.append(row)

        if not rows:
            return pd.DataFrame(columns=self.get_columns())

        df = pd.DataFrame(rows)
        return df

    def get_columns(self) -> List[Text]:
        """Return base column names for the metric_data table."""
        return [
            "pql_query",
            "start_ts",
            "end_ts",
            "step",
            "timeout",
            "timestamp",
            "value",
            "labels_json",
        ]

    def add(self, data: List[dict], **kwargs) -> None:
        """Add metrics to Prometheus Pushgateway.

        Args:
            data: List of dictionaries containing metric data
                Required fields:
                    - metric_name (VARCHAR): Name of the metric (must start with a letter)
                    - value (DOUBLE): Numeric value of the metric
                Optional fields:
                    - labels_json (JSON/TEXT): JSON string or dict of label key-value pairs
                    - timestamp (INTEGER): Unix timestamp in seconds (default: current time)
                    - job (VARCHAR): Job name for Pushgateway grouping (default: from config)

        Raises:
            ValueError: If required fields are missing or invalid
            Exception: If push to Pushgateway fails
        """
        import time

        for row in data:
            # Validate required fields
            if "metric_name" not in row:
                raise ValueError("metric_name is required for INSERT")
            if "value" not in row:
                raise ValueError("value is required for INSERT")

            metric_name = str(row["metric_name"])
            value = row["value"]

            # Parse labels_json
            labels = {}
            if "labels_json" in row and row["labels_json"]:
                try:
                    if isinstance(row["labels_json"], str):
                        labels = json.loads(row["labels_json"])
                    elif isinstance(row["labels_json"], dict):
                        labels = row["labels_json"]
                    else:
                        raise ValueError(
                            f"labels_json must be a JSON string or dict, got {type(row['labels_json'])}"
                        )
                except json.JSONDecodeError as e:
                    raise ValueError(f"Invalid JSON in labels_json: {str(e)}")

            # Get timestamp (default to current time)
            timestamp = row.get("timestamp")
            if timestamp is None:
                timestamp = int(time.time())
            else:
                # Convert to int if it's a float
                timestamp = int(float(timestamp))

            # Get job name (default from handler config)
            job = row.get("job", self.handler.pushgateway_job)

            # Build Pushgateway endpoint
            endpoint = f"/metrics/job/{job}"

            # Extract instance label for Pushgateway instance grouping
            instance = None
            if "instance" in labels:
                instance = labels["instance"]
                endpoint = f"{endpoint}/instance/{instance}"
                # Create a copy of labels without instance for formatting
                labels_for_formatting = {
                    k: v for k, v in labels.items() if k != "instance"
                }
            else:
                labels_for_formatting = labels

            # Format metric in Prometheus text format
            metric_text = self._format_metric_for_pushgateway(
                metric_name, value, labels_for_formatting, timestamp
            )

            try:
                self.handler.call_pushgateway_api(endpoint, metric_text)
                logger.info(f"Successfully pushed metric {metric_name} to Pushgateway")
            except Exception as e:
                logger.error(
                    f"Failed to push metric {metric_name} to Pushgateway: {str(e)}"
                )
                raise Exception(f"Failed to push metric to Pushgateway: {str(e)}")

    def _format_metric_for_pushgateway(
        self, metric_name: str, value: Any, labels: Dict[str, Any], timestamp: int
    ) -> str:
        """Format a metric in Prometheus text format for Pushgateway.

        Args:
            metric_name: Name of the metric (must follow Prometheus naming conventions)
            value: Metric value (must be numeric)
            labels: Dictionary of label key-value pairs
            timestamp: Unix timestamp in seconds

        Returns:
            Formatted metric string in Prometheus text format

        Raises:
            ValueError: If metric name or value is invalid
        """
        # Validate metric name (Prometheus naming rules)
        if not metric_name or not metric_name[0].isalpha():
            raise ValueError(
                f"Invalid metric name: {metric_name}. Must start with a letter."
            )

        # Format labels
        label_parts = []
        for key, val in sorted(labels.items()):
            # Escape special characters in label values
            val_str = (
                str(val).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
            )
            label_parts.append(f'{key}="{val_str}"')

        label_str = ""
        if label_parts:
            label_str = "{" + ", ".join(label_parts) + "}"

        # Format value (ensure it's numeric)
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            raise ValueError(f"Metric value must be numeric, got: {value}")

        # Prometheus text format: metric_name{labels} value timestamp
        # Timestamp should be in milliseconds for Pushgateway
        timestamp_ms = timestamp * 1000

        return f"{metric_name}{label_str} {numeric_value} {timestamp_ms}\n"

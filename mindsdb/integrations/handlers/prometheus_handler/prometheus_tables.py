from typing import List, Dict, Text, Any, Optional, Tuple
import pandas as pd
import requests
from datetime import datetime, timedelta
import json

from mindsdb.integrations.libs.api_handler import APIResource
from mindsdb.integrations.utilities.sql_utils import FilterCondition, SortColumn
from mindsdb.utilities import log

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
                        first_entry = metric_metadata[0] if isinstance(metric_metadata, list) else metric_metadata
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
                    elif condition.column == "metric_name" and condition.op.value == "!=":
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
                "COLUMN_DESCRIPTION": "Label name",
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
            label_names = response.get("data", [])

            # Get all metric names
            metric_response = self.handler.call_prometheus_api("/api/v1/label/__name__/values")
            metric_names = metric_response.get("data", [])

            labels_data = []
            # Sample a subset of metrics to avoid too much data
            sample_metrics = metric_names[:100] if len(metric_names) > 100 else metric_names

            for metric_name in sample_metrics:
                # Query a sample of the metric to get label combinations
                query = f'{{__name__="{metric_name}"}}'
                query_response = self.handler.call_prometheus_api("/api/v1/query", params={"query": query})

                if query_response.get("status") == "success":
                    results = query_response.get("data", {}).get("result", [])
                    for result in results[:10]:  # Limit to 10 samples per metric
                        labels = result.get("metric", {})
                        custom_labels = {k: v for k, v in labels.items() if k not in ["__name__", "job", "instance", "method", "status"]}

                        labels_data.append(
                            {
                                "metric_name": metric_name,
                                "label": json.dumps(list(labels.keys())),
                                "job": labels.get("job"),
                                "instance": labels.get("instance"),
                                "method": labels.get("method"),
                                "status": labels.get("status"),
                                "custom_labels": json.dumps(custom_labels) if custom_labels else None,
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
            return pd.DataFrame(columns=["metric_name", "label", "job", "instance", "method", "status", "custom_labels"])

    def get_columns(self) -> List[Text]:
        """Return column names for the labels table."""
        return ["metric_name", "label", "job", "instance", "method", "status", "custom_labels"]


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
                labels = target.get("labels", [])
                # Convert labels list to dict
                labels_dict = {label.get("name"): label.get("value") for label in labels if label.get("name") and label.get("value")}

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


class MetricDataTable(APIResource):
    """Prometheus Metric Data table - query metric data with PromQL."""

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
        base_columns = [
            {
                "TABLE_NAME": "metric_data",
                "COLUMN_NAME": "pql_query",
                "DATA_TYPE": "VARCHAR",
                "COLUMN_DESCRIPTION": "PromQL query string",
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
        ]
        # Note: Label columns are dynamic and will be added based on the query results
        return base_columns

    def list(
        self,
        conditions: List[FilterCondition] = None,
        limit: int = None,
        sort: List[SortColumn] = None,
        targets: List[str] = None,
    ) -> pd.DataFrame:
        """Query metric data using PromQL."""
        try:
            # Extract query parameters from conditions
            pql_query = None
            start_ts = None
            end_ts = None
            step = None
            timeout = None
            label_filters = {}

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
                    else:
                        # This is likely a label filter
                        label_filters[col] = {"op": op, "value": val}

            if not pql_query:
                raise ValueError("pql_query parameter is required")

            # Build the PromQL query with label filters
            final_query = self._build_query_with_label_filters(pql_query, label_filters)

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

                response = self.handler.call_prometheus_api("/api/v1/query_range", params=params)
                return self._process_range_query_response(response, pql_query, start_ts, end_ts, step, timeout, label_filters)
            else:
                # Instant query
                if start_ts:
                    params["time"] = self._parse_timestamp(start_ts)

                response = self.handler.call_prometheus_api("/api/v1/query", params=params)
                return self._process_instant_query_response(response, pql_query, start_ts, end_ts, step, timeout, label_filters)

        except Exception as e:
            logger.error(f"Error querying metric data: {str(e)}")
            raise

    def _build_query_with_label_filters(self, base_query: str, label_filters: Dict[str, Dict[str, Any]]) -> str:
        """Build PromQL query with label filters applied.

        Args:
            base_query: Base PromQL query
            label_filters: Dictionary of label filters {label_name: {op: operator, value: value}}

        Returns:
            Modified PromQL query with label filters
        """
        if not label_filters:
            return base_query

        # Try to extract metric name and add label filters
        # This is a simplified approach - for complex queries, we might need to parse the PromQL
        # For now, we'll add label filters to the base query if it's a simple metric selector

        # Check if the query already has label selectors
        if "{" in base_query and "}" in base_query:
            # Query already has label selectors, try to merge
            # This is complex, so for now we'll apply filters in memory after querying
            return base_query
        else:
            # Simple metric name query, add label filters
            label_parts = []
            for label_name, filter_info in label_filters.items():
                op = filter_info["op"]
                val = filter_info["value"]

                if op == "=":
                    label_parts.append(f'{label_name}="{val}"')
                elif op == "!=":
                    label_parts.append(f'{label_name}!="{val}"')
                elif op == ">":
                    label_parts.append(f'{label_name}>{val}')
                elif op == "<":
                    label_parts.append(f'{label_name}<{val}')
                elif op == ">=":
                    label_parts.append(f'{label_name}>={val}')
                elif op == "<=":
                    label_parts.append(f'{label_name}<={val}')

            if label_parts:
                # Wrap the query in a label selector
                if base_query.endswith("}"):
                    # Already has braces, insert before closing
                    base_query = base_query[:-1] + ", " + ", ".join(label_parts) + "}"
                else:
                    # Add braces with label filters
                    base_query = f"{base_query}{{{', '.join(label_parts)}}}"

        return base_query

    def _parse_timestamp(self, ts: Any) -> str:
        """Parse timestamp to Prometheus format (Unix timestamp or RFC3339).

        Args:
            ts: Timestamp as string (ISO format) or number (Unix timestamp)

        Returns:
            String representation of timestamp
        """
        if isinstance(ts, (int, float)):
            return str(ts)
        elif isinstance(ts, str):
            # Try to parse as ISO format
            try:
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                return str(dt.timestamp())
            except:
                # Try as Unix timestamp string
                try:
                    float(ts)
                    return ts
                except:
                    raise ValueError(f"Invalid timestamp format: {ts}")
        else:
            raise ValueError(f"Invalid timestamp type: {type(ts)}")

    def _process_instant_query_response(
        self, response: Dict[str, Any], pql_query: str, start_ts: Any, end_ts: Any, step: Any, timeout: Any, label_filters: Dict
    ) -> pd.DataFrame:
        """Process instant query response into DataFrame."""
        if response.get("status") != "success":
            raise Exception(f"Prometheus query failed: {response.get('error', 'Unknown error')}")

        results = response.get("data", {}).get("result", [])
        rows = []

        for result in results:
            metric = result.get("metric", {})
            value = result.get("value", [None, None])
            timestamp = value[0] if len(value) > 0 else None
            metric_value = value[1] if len(value) > 1 else None

            # Apply label filters in memory if not applied in query
            if label_filters:
                should_include = True
                for label_name, filter_info in label_filters.items():
                    label_value = metric.get(label_name)
                    if label_value is None:
                        should_include = False
                        break

                    op = filter_info["op"]
                    val = filter_info["value"]

                    if op == "=" and label_value != val:
                        should_include = False
                        break
                    elif op == "!=" and label_value == val:
                        should_include = False
                        break
                    elif op in [">", "<", ">=", "<="]:
                        # Try numeric comparison
                        try:
                            label_num = float(label_value)
                            val_num = float(val)
                            if op == ">" and not (label_num > val_num):
                                should_include = False
                                break
                            elif op == "<" and not (label_num < val_num):
                                should_include = False
                                break
                            elif op == ">=" and not (label_num >= val_num):
                                should_include = False
                                break
                            elif op == "<=" and not (label_num <= val_num):
                                should_include = False
                                break
                        except (ValueError, TypeError):
                            # If not numeric, skip this filter (or could do string comparison)
                            logger.warning(f"Label value '{label_value}' or filter value '{val}' is not numeric, skipping comparison")
                            continue

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
            }

            # Add all label columns
            for label_name, label_value in metric.items():
                if label_name != "__name__":
                    row[label_name] = label_value

            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df

    def _process_range_query_response(
        self, response: Dict[str, Any], pql_query: str, start_ts: Any, end_ts: Any, step: Any, timeout: Any, label_filters: Dict
    ) -> pd.DataFrame:
        """Process range query response into DataFrame."""
        if response.get("status") != "success":
            raise Exception(f"Prometheus query failed: {response.get('error', 'Unknown error')}")

        results = response.get("data", {}).get("result", [])
        rows = []

        for result in results:
            metric = result.get("metric", {})
            values = result.get("values", [])

            # Apply label filters in memory if not applied in query
            if label_filters:
                should_include = True
                for label_name, filter_info in label_filters.items():
                    label_value = metric.get(label_name)
                    if label_value is None:
                        should_include = False
                        break

                    op = filter_info["op"]
                    val = filter_info["value"]

                    if op == "=" and label_value != val:
                        should_include = False
                        break
                    elif op == "!=" and label_value == val:
                        should_include = False
                        break
                    elif op in [">", "<", ">=", "<="]:
                        # Try numeric comparison
                        try:
                            label_num = float(label_value)
                            val_num = float(val)
                            if op == ">" and not (label_num > val_num):
                                should_include = False
                                break
                            elif op == "<" and not (label_num < val_num):
                                should_include = False
                                break
                            elif op == ">=" and not (label_num >= val_num):
                                should_include = False
                                break
                            elif op == "<=" and not (label_num <= val_num):
                                should_include = False
                                break
                        except (ValueError, TypeError):
                            # If not numeric, skip this filter (or could do string comparison)
                            logger.warning(f"Label value '{label_value}' or filter value '{val}' is not numeric, skipping comparison")
                            continue

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
                }

                # Add all label columns
                for label_name, label_value in metric.items():
                    if label_name != "__name__":
                        row[label_name] = label_value

                rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        return df

    def get_columns(self) -> List[Text]:
        """Return base column names for the metric_data table.
        Note: Label columns are dynamic and will be added based on query results.
        """
        return ["pql_query", "start_ts", "end_ts", "step", "timeout", "timestamp", "value"]


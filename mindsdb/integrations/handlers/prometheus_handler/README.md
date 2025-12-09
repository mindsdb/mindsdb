# Prometheus Handler

MindsDB handler for querying Prometheus metrics and metadata.

## Prometheus

Prometheus is an open-source systems monitoring and alerting toolkit. This handler allows you to query Prometheus metrics, labels, scrape targets, and metric data using SQL.

## Implementation

This handler was implemented using the `requests` library to interact with the Prometheus HTTP API.

## Configuration

The handler requires the following connection parameters:

- `prometheus_url` (required): The base URL of the Prometheus server (e.g., `http://localhost:9090`)
- `username` (optional): Username for basic authentication
- `password` (optional): Password for basic authentication (required if username is provided)
- `bearer_token` (optional): Bearer token for token-based authentication (alternative to username/password)
- `timeout` (optional): Request timeout in seconds (default: 10)

**Note:** If both bearer_token and username/password are provided, bearer_token will be used.

## Usage

### Connect to Prometheus (No Authentication)

```sql
CREATE DATABASE prometheus_db
WITH ENGINE = "prometheus",
PARAMETERS = {
    "prometheus_url": "http://localhost:9090",
    "timeout": 10
};
```

### Connect to Prometheus (Basic Authentication)

```sql
CREATE DATABASE prometheus_db
WITH ENGINE = "prometheus",
PARAMETERS = {
    "prometheus_url": "http://localhost:9090",
    "username": "admin",
    "password": "secret",
    "timeout": 10
};
```

### Connect to Prometheus (Bearer Token)

```sql
CREATE DATABASE prometheus_db
WITH ENGINE = "prometheus",
PARAMETERS = {
    "prometheus_url": "http://localhost:9090",
    "bearer_token": "your_bearer_token_here",
    "timeout": 10
};
```

## Virtual Tables

The handler provides four virtual tables:

### 1. `metrics`

Lists all available metrics with their metadata.

**Columns:**
- `metric_name` (VARCHAR): Name of the metric
- `type` (VARCHAR): Metric type (counter, gauge, histogram, summary, etc.)
- `help` (TEXT): Help text describing the metric
- `unit` (VARCHAR): Unit of measurement for the metric

**Example:**
```sql
SELECT * FROM prometheus_db.metrics
WHERE metric_name LIKE 'http_%';
```

### 2. `labels`

Lists all labels available for metrics with common label values.

**Columns:**
- `metric_name` (VARCHAR): Name of the metric
- `label` (VARCHAR): JSON string of all label names
- `job` (VARCHAR): Job label value
- `instance` (VARCHAR): Instance label value
- `method` (VARCHAR): Method label value
- `status` (VARCHAR): Status label value
- `custom_labels` (TEXT): JSON string of custom label key-value pairs

**Example:**
```sql
SELECT * FROM prometheus_db.labels
WHERE metric_name = 'http_requests_total';
```

### 3. `scrape_targets`

Lists all scrape targets with their health status.

**Columns:**
- `target_labels` (TEXT): JSON string of target label key-value pairs
- `health` (VARCHAR): Health status (up, down, unknown)
- `scrape_url` (VARCHAR): URL from which the target is scraped

**Example:**
```sql
SELECT * FROM prometheus_db.scrape_targets
WHERE health = 'up';
```

### 4. `metric_data`

Query metric data using PromQL with label filtering support.

**Base Columns:**
- `pql_query` (VARCHAR): PromQL query string (required)
- `start_ts` (TIMESTAMP): Start timestamp for range queries
- `end_ts` (TIMESTAMP): End timestamp for range queries
- `step` (VARCHAR): Query resolution step width (e.g., '10s', '1m', '5m')
- `timeout` (VARCHAR): Query timeout (e.g., '10s')
- `timestamp` (TIMESTAMP): Timestamp of the data point
- `value` (DOUBLE): Metric value

**Dynamic Columns:**
- One column for each label in the query results (e.g., `job`, `instance`, `method`, `status`, etc.)

**Example - Instant Query:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'up';
```

**Example - Range Query:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(http_requests_total[5m])'
  AND start_ts = '2024-01-01T00:00:00Z'
  AND end_ts = '2024-01-01T01:00:00Z'
  AND step = '10s';
```

**Example - With Label Filtering:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(http_requests_total[5m])'
  AND start_ts = '2024-01-01T00:00:00Z'
  AND end_ts = '2024-01-01T01:00:00Z'
  AND step = '10s'
  AND status = '200'
  AND method = 'GET';
```

**Example - With Numeric Label Filtering:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(metric[5m])'
  AND somelabel > 10
  AND somelabel < 100
  AND someother_label = 100
  AND step = '10s';
```

## Label Filtering

The `metric_data` table supports filtering by labels in the WHERE clause. Label filters are applied in two ways:

1. **In PromQL Query**: Simple label filters (equality) are added to the PromQL query when possible
2. **In Memory**: Complex filters (comparisons, multiple conditions) are applied to the results after querying

This allows for flexible querying while maintaining performance for simple cases.

## Notes

- Timestamps can be provided as ISO 8601 strings (e.g., '2024-01-01T00:00:00Z') or Unix timestamps
- The `step` parameter is required for range queries
- Label columns are dynamically added based on the labels present in the query results
- For large result sets, consider using LIMIT to restrict the number of rows returned


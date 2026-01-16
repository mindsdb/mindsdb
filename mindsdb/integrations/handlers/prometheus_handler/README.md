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
- `pushgateway_url` (optional): The base URL of the Prometheus Pushgateway (e.g., `http://localhost:9091`). If not provided, will attempt to derive from prometheus_url by changing port 9090 to 9091.
- `pushgateway_job` (optional): Default job name for Pushgateway metrics (default: 'default')

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

### Connect to Prometheus with Pushgateway

```sql
CREATE DATABASE prometheus_db
WITH ENGINE = "prometheus",
PARAMETERS = {
    "prometheus_url": "http://localhost:9090",
    "pushgateway_url": "http://localhost:9091",
    "pushgateway_job": "my_app",
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

Query metric data using PromQL with JSON path label filtering support.

**Base Columns:**
- `pql_query` (VARCHAR): PromQL query string (required)
- `start_ts` (TIMESTAMP): Start timestamp for range queries
- `end_ts` (TIMESTAMP): End timestamp for range queries
- `step` (VARCHAR): Query resolution step width (e.g., '10s', '1m', '5m')
- `timeout` (VARCHAR): Query timeout (e.g., '10s')
- `timestamp` (TIMESTAMP): Timestamp of the data point
- `value` (DOUBLE): Metric value
- `labels_json` (JSON): JSON object of all labels and their values for this row

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

**Example - With JSON Path Label Filtering (PostgreSQL style):**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(http_requests_total[5m])'
  AND start_ts = '2024-01-01T00:00:00Z'
  AND end_ts = '2024-01-01T01:00:00Z'
  AND step = '10s'
  AND labels_json->>'status' = '200'
  AND labels_json->>'method' = 'GET';
```

**Example - With JSON Path Label Filtering (MySQL style):**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(http_requests_total[5m])'
  AND start_ts = '2024-01-01T00:00:00Z'
  AND end_ts = '2024-01-01T01:00:00Z'
  AND step = '10s'
  AND JSON_EXTRACT(labels_json, '$.status') = '200'
  AND JSON_EXTRACT(labels_json, '$.method') = 'GET';
```

**Example - With Numeric JSON Path Filtering:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(metric[5m])'
  AND labels_json->>'somelabel' > 10
  AND labels_json->>'somelabel' < 100
  AND labels_json->>'someother_label' = 100
  AND step = '10s';
```

**Example - With BETWEEN Operator:**
```sql
SELECT * FROM prometheus_db.metric_data
WHERE pql_query = 'rate(metric[5m])'
  AND labels_json->>'somelabel' BETWEEN 10 AND 100
  AND step = '10s';
```

**Example - Selecting labels_json:**
```sql
SELECT timestamp, value, labels_json FROM prometheus_db.metric_data
WHERE pql_query = 'up'
  AND start_ts = '2024-01-01T00:00:00Z';
```

The `labels_json` column contains a JSON object with all labels and their values for each row, excluding the `__name__` label. For example:
```json
{"job": "prometheus", "instance": "localhost:9090", "status": "up"}
```

## Label Filtering

The `metric_data` table supports filtering by labels using JSON path expressions in the WHERE clause. Two syntaxes are supported:

1. **PostgreSQL style**: `labels_json->>'label_name'`
2. **MySQL style**: `JSON_EXTRACT(labels_json, '$.label_name')`

Label filters are applied in two ways:

1. **Pushdown to PromQL**: Simple equality filters (`=`, `!=`) are added to the PromQL query when possible for better performance
2. **In-Memory Filtering**: Complex filters (`>`, `<`, `>=`, `<=`, `BETWEEN`) are applied to the results after querying

**Note**: Direct label column filtering (e.g., `WHERE status = '200'`) is deprecated. Use JSON path syntax instead (e.g., `WHERE labels_json->>'status' = '200'`).

## Inserting Metrics (Pushgateway)

The `metric_data` table supports INSERT operations to push metrics to Prometheus Pushgateway. This is useful for batch jobs, short-lived processes, or applications that need to push metrics rather than being scraped.

### Prerequisites

1. **Pushgateway Setup**: Ensure Prometheus Pushgateway is running (default: `http://localhost:9091`)
2. **Configuration**: Optionally configure `pushgateway_url` and `pushgateway_job` in connection parameters

### INSERT Syntax

```sql
INSERT INTO metric_data (metric_name, value, labels_json, timestamp, job) 
VALUES ('http_requests_total', 100, '{"method":"GET","status":"200"}', 1234567890, 'my_job');
```

### Required Fields

- `metric_name` (VARCHAR): Name of the metric (must start with a letter)
- `value` (DOUBLE): Numeric value of the metric

### Optional Fields

- `labels_json` (JSON/TEXT): JSON string or object containing label key-value pairs (default: empty object)
- `timestamp` (INTEGER): Unix timestamp in seconds (default: current time)
- `job` (VARCHAR): Job name for Pushgateway grouping (default: from connection config or 'default')

### INSERT Examples

**Basic Insert (minimal fields):**
```sql
INSERT INTO metric_data (metric_name, value) 
VALUES ('cpu_usage', 85.5);
```

**Insert with Labels:**
```sql
INSERT INTO metric_data (metric_name, value, labels_json) 
VALUES ('http_requests_total', 100, '{"method":"GET","status":"200","endpoint":"/api/users"}');
```

**Insert with Timestamp:**
```sql
INSERT INTO metric_data (metric_name, value, labels_json, timestamp) 
VALUES ('temperature', 23.5, '{"sensor":"sensor1","location":"room1"}', 1704067200);
```

**Insert with Custom Job:**
```sql
INSERT INTO metric_data (metric_name, value, labels_json, job) 
VALUES ('batch_job_duration', 45.2, '{"job_type":"data_processing"}', 'batch_jobs');
```

**Batch Insert (multiple metrics):**
```sql
INSERT INTO metric_data (metric_name, value, labels_json) 
VALUES 
  ('http_requests_total', 100, '{"method":"GET","status":"200"}'),
  ('http_requests_total', 50, '{"method":"POST","status":"201"}'),
  ('http_requests_total', 5, '{"method":"GET","status":"500"}');
```

### Pushgateway Integration

Metrics are pushed to Pushgateway using the endpoint:
- `/metrics/job/<job_name>` - Basic job grouping
- `/metrics/job/<job_name>/instance/<instance>` - If `instance` label is provided

The metrics are formatted in Prometheus text format and sent via HTTP POST to the Pushgateway.

**Note**: After pushing metrics to Pushgateway, ensure Prometheus is configured to scrape the Pushgateway as a target in your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'pushgateway'
    static_configs:
      - targets: ['localhost:9091']
```

## Notes

- Timestamps can be provided as ISO 8601 strings (e.g., '2024-01-01T00:00:00Z') or Unix timestamps
- The `step` parameter is required for range queries
- Use JSON path expressions (`labels_json->>'label_name'` or `JSON_EXTRACT(labels_json, '$.label_name')`) to filter by labels
- For large result sets, consider using LIMIT to restrict the number of rows returned
- Simple equality filters are pushed down to PromQL for better performance, while complex filters are applied in-memory
- When inserting metrics, the `instance` label (if provided) will be used for Pushgateway instance grouping
- Metric names must start with a letter and follow Prometheus naming conventions


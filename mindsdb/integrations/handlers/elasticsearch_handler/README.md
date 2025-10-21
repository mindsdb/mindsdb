---
title: Elasticsearch
sidebarTitle: Elasticsearch
---

This documentation describes the integration of MindsDB with [Elasticsearch](https://www.elastic.co/elasticsearch/), a distributed search and analytics engine.
The integration allows MindsDB to access data stored in Elasticsearch indices and enhance Elasticsearch with AI capabilities.

## Architecture

This handler uses a **SQL-first architecture** with automatic fallback:

1. **Primary**: Elasticsearch SQL API for maximum performance and compatibility
2. **Fallback**: Search API for array-containing indexes with automatic array-to-JSON conversion
3. **Security**: SSL/TLS support with certificate validation
4. **Efficiency**: Memory-efficient pagination for large datasets

The handler automatically detects when SQL queries encounter array fields and seamlessly falls back to the Search API, converting arrays to JSON strings for SQL compatibility. This approach provides the best performance while handling all Elasticsearch data types.

## Prerequisites

Before proceeding, ensure the following prerequisites are met:

1. Install MindsDB locally via [Docker](https://docs.mindsdb.com/setup/self-hosted/docker) or [Docker Desktop](https://docs.mindsdb.com/setup/self-hosted/docker-desktop).
2. To connect Elasticsearch to MindsDB, install the required dependencies following [this instruction](https://docs.mindsdb.com/setup/self-hosted/docker#install-dependencies).

## Connection

Establish a connection to your Elasticsearch cluster from MindsDB by executing the following SQL command:

```sql
CREATE DATABASE elasticsearch_conn
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
    "hosts": "localhost:9200",
    "user": "elastic", 
    "password": "changeme"
};
```

Required connection parameters include the following:

* `hosts`: The Elasticsearch host(s) in format "host:port". For multiple hosts, use comma separation like "host1:port1,host2:port2".

Optional connection parameters include the following:

* `user`: The username for Elasticsearch authentication.
* `password`: The password for Elasticsearch authentication.
* `api_key`: API key for authentication (alternative to user/password).
* `cloud_id`: Elastic Cloud deployment ID for hosted Elasticsearch.
* `ca_certs`: Path to CA certificate file for SSL verification.
* `client_cert`: Path to client certificate file for SSL authentication.
* `client_key`: Path to client private key file for SSL authentication.
* `verify_certs`: Boolean to enable/disable SSL certificate verification (default: true).
* `timeout`: Request timeout in seconds.

## Usage

The following usage examples utilize the connection to Elasticsearch made via the `CREATE DATABASE` statement and named `elasticsearch_conn`.

Retrieve data from a specified index by providing the integration name and index name:

```sql
SELECT *
FROM elasticsearch_conn.products
LIMIT 10;
```

Query with filtering and aggregation:

```sql
SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
FROM elasticsearch_conn.products 
WHERE price > 100
GROUP BY category
ORDER BY product_count DESC;
```

Run queries with array fields (automatically converted to JSON strings):

```sql
SELECT product_name, tags, categories 
FROM elasticsearch_conn.products 
WHERE product_id = '12345';
```

<Tip>
**Array Field Support**

The Elasticsearch handler automatically detects and converts array fields to JSON strings for SQL compatibility. This prevents "Arrays not supported" errors while preserving the original data structure.
</Tip>

## Schema Discovery

List available indices and columns:

```sql
-- List all indices (tables)
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'elasticsearch_conn';

-- Get column information
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'products' AND table_schema = 'elasticsearch_conn';
```

## Data Catalog Support

The Elasticsearch handler provides Data Catalog support for enterprise requirements with three key methods:

### Column Statistics

Get detailed statistical information about index fields:

```sql
-- All columns
SELECT * FROM mindsdb.get_column_statistics('elasticsearch_conn', 'products');

-- Specific column
SELECT * FROM mindsdb.get_column_statistics('elasticsearch_conn', 'products', 'price');
```

Returns: `column_name`, `data_type`, `null_count`, `distinct_count`, `min`, `max`, `avg`

Supported field types: numeric (with min/max/avg), keyword, text, date, geo_point, ip, nested objects (flattened with dot notation).

### Primary Keys

```sql
SELECT * FROM mindsdb.get_primary_keys('elasticsearch_conn', 'products');
```

Returns `_id` as the primary key (Elasticsearch's document identifier).

### Foreign Keys

```sql
SELECT * FROM mindsdb.get_foreign_keys('elasticsearch_conn', 'products');
```

Returns empty (NoSQL databases don't have foreign keys).

## Troubleshooting

<Warning>
`Database Connection Error`

* **Symptoms**: Failure to connect MindsDB with the Elasticsearch cluster.
* **Checklist**:
    1. Make sure the Elasticsearch cluster is active and accessible.
    2. Confirm that host, port, user, and password are correct. Try a direct Elasticsearch connection.
    3. Ensure a stable network between MindsDB and Elasticsearch.
    4. Check if authentication is required and credentials are valid.
</Warning>

<Warning>
`Arrays Not Supported Error`

* **Symptoms**: SQL queries failing with "Arrays are not supported" message.
* **Solution**: This is automatically handled by the integration. Array fields are converted to JSON strings for SQL compatibility.
* **Note**: If you still encounter this error, the handler will automatically fall back to the Search API.
</Warning>

<Warning>
`SHOW TABLES returns empty or fails`

* **Symptoms**: `SHOW TABLES FROM elasticsearch_conn` returns no results or fails.
* **Solution**: Use the information_schema alternative:
    ```sql
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'elasticsearch_conn';
    ```
</Warning>

## Limitations

* **JOINs**: Not supported due to Elasticsearch architecture limitations.
* **Complex Subqueries**: Limited by Elasticsearch's SQL capabilities.
* **Real-time Data**: Elasticsearch has near-real-time search characteristics due to refresh intervals.
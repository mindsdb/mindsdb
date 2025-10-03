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

## Advanced Queries

The Elasticsearch handler supports a wide range of advanced SQL operations and Elasticsearch-specific functionality:

### Text Search and Filtering

```sql
-- Full-text search using LIKE for pattern matching
SELECT product_name, description, price 
FROM elasticsearch_conn.products 
WHERE description LIKE '%wireless%' 
AND price BETWEEN 50 AND 500
ORDER BY price ASC;

-- Case-insensitive search
SELECT title, author, publication_date 
FROM elasticsearch_conn.books 
WHERE LOWER(title) LIKE '%python%' 
OR LOWER(author) LIKE '%python%';
```

### Date and Time Queries

```sql
-- Date range filtering
SELECT order_id, customer_name, order_date, total_amount 
FROM elasticsearch_conn.orders 
WHERE order_date >= '2024-01-01' 
AND order_date < '2024-12-31'
ORDER BY order_date DESC;

-- Recent data queries
SELECT log_level, message, timestamp 
FROM elasticsearch_conn.application_logs 
WHERE timestamp >= NOW() - INTERVAL 24 HOUR
AND log_level IN ('ERROR', 'WARN');
```

### Aggregations and Analytics

```sql
-- Statistical aggregations
SELECT 
    category,
    COUNT(*) as total_products,
    AVG(price) as average_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    SUM(stock_quantity) as total_stock
FROM elasticsearch_conn.products 
WHERE status = 'active'
GROUP BY category 
HAVING COUNT(*) > 10
ORDER BY average_price DESC;

-- Time-based aggregations
SELECT 
    DATE_FORMAT(created_at, '%Y-%m') as month,
    COUNT(*) as user_registrations,
    COUNT(DISTINCT country) as countries_count
FROM elasticsearch_conn.users 
WHERE created_at >= '2024-01-01'
GROUP BY DATE_FORMAT(created_at, '%Y-%m')
ORDER BY month;
```

### Working with Array and Nested Fields

```sql
-- Query documents with array fields (automatically handled)
SELECT 
    product_id,
    product_name,
    tags,  -- Array field converted to JSON string
    categories,  -- Array field converted to JSON string
    attributes  -- Object field converted to JSON string
FROM elasticsearch_conn.products 
WHERE product_id IN ('12345', '67890', '11111');

-- Search within array-like JSON strings (use JSON functions if available)
SELECT product_name, tags, price 
FROM elasticsearch_conn.products 
WHERE tags LIKE '%electronics%' 
OR tags LIKE '%gadget%';
```

### Complex Filtering

```sql
-- Multiple condition filtering
SELECT 
    user_id,
    email,
    last_login,
    account_status
FROM elasticsearch_conn.users 
WHERE account_status = 'active'
AND last_login >= DATE_SUB(NOW(), INTERVAL 30 DAY)
AND email LIKE '%@company.com'
ORDER BY last_login DESC;

-- Null and existence checks
SELECT product_id, product_name, description 
FROM elasticsearch_conn.products 
WHERE description IS NOT NULL 
AND price IS NOT NULL
AND stock_quantity > 0;
```

### Pagination and Limiting

```sql
-- Efficient pagination
SELECT product_id, product_name, price 
FROM elasticsearch_conn.products 
WHERE category = 'electronics'
ORDER BY created_at DESC 
LIMIT 20 OFFSET 100;

-- Top N queries
SELECT customer_id, customer_name, total_orders 
FROM elasticsearch_conn.customer_summary 
ORDER BY total_orders DESC 
LIMIT 10;
```

### Performance Optimization Tips

```sql
-- Use specific field selection instead of SELECT *
SELECT id, title, status 
FROM elasticsearch_conn.documents 
WHERE status = 'published'
LIMIT 1000;

-- Filter before aggregation for better performance
SELECT region, AVG(sales_amount) as avg_sales
FROM elasticsearch_conn.sales 
WHERE sale_date >= '2024-01-01'  -- Filter first
GROUP BY region;

-- Use EXISTS queries for field presence
SELECT document_id, title 
FROM elasticsearch_conn.documents 
WHERE content IS NOT NULL 
AND LENGTH(content) > 100;
```

### Integration with MindsDB AI Features

```sql
-- Use Elasticsearch data with MindsDB models
SELECT 
    p.product_id,
    p.product_name,
    p.description,
    m.predicted_category
FROM elasticsearch_conn.products p
JOIN mindsdb.product_classifier m
WHERE p.category IS NULL;

-- Sentiment analysis on Elasticsearch text data
SELECT 
    review_id,
    review_text,
    rating,
    sentiment_analysis.sentiment as predicted_sentiment
FROM elasticsearch_conn.product_reviews r
JOIN mindsdb.sentiment_analyzer sentiment_analysis
WHERE rating IS NOT NULL;
```

## Schema Discovery

Since `SHOW TABLES` and `DESCRIBE` may have limitations, use these alternatives:

```sql
-- List all indices (tables)
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'elasticsearch_conn';

-- Get column information
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'products' AND table_schema = 'elasticsearch_conn';
```

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
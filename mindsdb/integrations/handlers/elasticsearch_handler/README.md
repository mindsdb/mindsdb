# Elasticsearch Handler

![Elasticsearch](https://img.shields.io/badge/Elasticsearch-005571?style=for-the-badge&logo=elasticsearch&logoColor=white)
![MindsDB](https://img.shields.io/badge/MindsDB-brightgreen?style=for-the-badge)
![Python](https://img.shields.io/badge/python-3.8+-blue.svg?style=for-the-badge&logo=python&logoColor=white)

The Elasticsearch handler for MindsDB enables you to query Elasticsearch clusters using SQL syntax. This integration allows you to seamlessly connect to Elasticsearch indices and perform SQL operations on your search data.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Examples](#examples)
- [Limitations](#limitations)
- [Troubleshooting](#troubleshooting)
- [Contributing](#contributing)

## Features

### ✅ Supported Operations

- **SELECT queries** with WHERE, ORDER BY, LIMIT, OFFSET
- **Aggregations**: COUNT, SUM, AVG, MAX, MIN
- **GROUP BY** operations with terms aggregation
- **Filtering** on numeric, text, keyword, and date fields
- **Array field handling** - automatically converts arrays to JSON strings
- **Schema discovery** via MindsDB's information_schema
- **Large dataset support** using Elasticsearch scroll API
- **Unicode and special character support**
- **Multiple authentication methods**

### ❌ Not Supported

- **SHOW TABLES** / **DESCRIBE** commands (use alternatives below)
- **SQL JOINs** (limitation of Elasticsearch architecture)
- **Complex nested subqueries**
- **Advanced SQL functions not mappable to Elasticsearch DSL**

## Installation

### Prerequisites

- MindsDB installed and running
- Elasticsearch cluster (version 7.x or 8.x)
- Python 3.8 or higher

### Install Dependencies

The handler dependencies are automatically installed when MindsDB loads the integration:

```bash
pip install elasticsearch>=7.13.4 elasticsearch-dbapi>=0.2.9 pydantic pandas
```

## Usage

### 1. Create Connection

Connect to your Elasticsearch cluster:

```sql
CREATE DATABASE my_elasticsearch
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
    "hosts": "localhost:9200",
    "user": "elastic", 
    "password": "your_password"
};
```

### 2. Query Your Data

Once connected, you can query Elasticsearch indices using SQL:

```sql
-- Basic SELECT
SELECT * FROM my_elasticsearch.products LIMIT 10;

-- Filtering
SELECT name, price FROM my_elasticsearch.products 
WHERE price > 100 AND category = 'electronics';

-- Aggregations
SELECT category, COUNT(*) as product_count, AVG(price) as avg_price
FROM my_elasticsearch.products 
GROUP BY category 
ORDER BY product_count DESC;
```

## Configuration

### Connection Parameters

| Parameter | Required | Description | Example |
|-----------|----------|-------------|---------|
| `hosts` | Yes | Elasticsearch host(s) | `"localhost:9200"` or `"host1:9200,host2:9200"` |
| `user` | No | Username for authentication | `"elastic"` |
| `password` | No | Password for authentication | `"changeme"` |
| `api_key` | No | API key (alternative to user/password) | `"your_api_key"` |
| `cloud_id` | No | Elastic Cloud ID | `"deployment:dXMtY2VudHJhbDE..."` |

### Example Configurations

**Basic Connection:**
```sql
CREATE DATABASE local_es
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
    "hosts": "localhost:9200"
};
```

**With Authentication:**
```sql
CREATE DATABASE secure_es
WITH ENGINE = 'elasticsearch', 
PARAMETERS = {
    "hosts": "https://my-cluster.es.amazonaws.com:443",
    "user": "admin",
    "password": "secure_password"
};
```

**Elastic Cloud:**
```sql
CREATE DATABASE cloud_es
WITH ENGINE = 'elasticsearch',
PARAMETERS = {
    "cloud_id": "deployment:dXMtY2VudHJhbDE...",
    "api_key": "your_cloud_api_key"
};
```

## Examples

### Schema Discovery

Since `SHOW TABLES` and `DESCRIBE` have limitations, use these alternatives:

```sql
-- List all indices (tables)
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'my_elasticsearch';

-- Get column information
SELECT column_name, data_type FROM information_schema.columns 
WHERE table_name = 'products' AND table_schema = 'my_elasticsearch';
```

### Advanced Queries

```sql
-- Date range filtering
SELECT title, publish_date FROM my_elasticsearch.articles 
WHERE publish_date >= '2023-01-01' AND publish_date <= '2023-12-31';

-- Text search with LIKE
SELECT name, description FROM my_elasticsearch.products 
WHERE name LIKE '%laptop%' OR description LIKE '%computer%';

-- Aggregation example
SELECT 
    status,
    COUNT(*) as order_count,
    SUM(total_amount) as total_revenue
FROM my_elasticsearch.orders 
WHERE created_at >= '2023-01-01'
GROUP BY status
ORDER BY total_revenue DESC;
```

### Working with Arrays

The handler automatically converts Elasticsearch arrays to JSON strings:

```sql
-- Query will return arrays as JSON strings
SELECT product_name, tags, categories FROM my_elasticsearch.products 
WHERE product_id = '12345';

-- Result: tags = '["electronics", "laptop", "gaming"]'
```

## Limitations

### Known Issues

1. **SHOW TABLES FROM database** - May not work due to MindsDB core routing
   - **Workaround**: Use `information_schema.tables` or direct API calls

2. **DESCRIBE table** - May fail for some configurations  
   - **Workaround**: Use `information_schema.columns` or mapping API

3. **Array Fields** - Converted to JSON strings for SQL compatibility
   - **Note**: This is intentional to prevent "Arrays not supported" errors

### Elasticsearch Limitations

- **No JOINs**: Elasticsearch doesn't support table joins
- **Limited Subqueries**: Complex nested queries may not translate properly
- **Real-time**: Elasticsearch has near-real-time search characteristics (refresh interval)

For a detailed guide on the limitations of the Elasticsearch SQL API, refer to the [official documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/sql-limitations.html).

## Troubleshooting

### Common Issues

**Connection Failed:**
```
Error: Failed to connect to Elasticsearch
```
- Verify host and port are correct
- Check authentication credentials
- Ensure Elasticsearch is running and accessible

**Arrays Not Supported:**
```
Error: Arrays are not supported
```
- This is handled automatically by the handler
- Arrays are converted to JSON strings
- If you see this error, please report it as a bug

**Query Too Complex:**
```
Error: Query too complex to translate
```
- Simplify the query by removing complex subqueries
- Use multiple simpler queries instead
- Check if the SQL operation is supported by Elasticsearch

### Getting Help

1. Check the [MindsDB Documentation](https://docs.mindsdb.com/)
2. Join the [MindsDB Community Slack](https://mindsdb.com/joincommunity)
3. Open an issue on [GitHub](https://github.com/mindsdb/mindsdb/issues)

## Contributing

We welcome contributions! Here's how you can help:

1. **Report Bugs**: Open an issue with details about the problem
2. **Suggest Features**: Share ideas for new functionality
3. **Submit PRs**: Fix bugs or add features
4. **Improve Docs**: Help make documentation clearer

### Development Setup

```bash
# Clone the repository
git clone https://github.com/mindsdb/mindsdb.git
cd mindsdb

# Install in development mode
pip install -e .
```

### Running Tests

```bash
# Test the handler functionality (run from handler directory)
cd mindsdb/integrations/handlers/elasticsearch_handler
python -c "from elasticsearch_handler import ElasticsearchHandler; print('Handler loads successfully')"

# Or test from main MindsDB directory
python -c "
import sys
sys.path.append('mindsdb/integrations/handlers/elasticsearch_handler')
from elasticsearch_handler import ElasticsearchHandler
print('Handler loads successfully')
"

# Run MindsDB integration tests
python -m pytest tests/unit/handlers/ -k elasticsearch -v
```

## License

This handler is part of MindsDB and is licensed under the [GNU GPL v3.0](https://github.com/mindsdb/mindsdb/blob/main/LICENSE).

---

**Need Help?** Join our community:
- [Documentation](https://docs.mindsdb.com/)
- [Community Slack](https://mindsdb.com/joincommunity) 
- [GitHub Discussions](https://github.com/mindsdb/mindsdb/discussions)
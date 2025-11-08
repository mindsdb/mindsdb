# Multi-Format API Handler

The Multi-Format API handler allows you to fetch and parse data from web APIs and pages that return data in various formats including XML, JSON, and CSV.

## Features

- **Automatic Format Detection**: Detects format from Content-Type headers or URL extensions
- **Multiple Format Support**: JSON, XML (including RSS/Atom feeds), and CSV
- **Nested Data Handling**: Flattens nested JSON/XML structures automatically
- **Custom Headers**: Support for authentication headers and custom request headers
- **Simple SQL Interface**: Query any API endpoint using familiar SQL syntax

## Supported Formats

### JSON
- `application/json`
- Nested objects automatically flattened
- Arrays converted to rows

### XML
- `application/xml`, `text/xml`
- RSS/Atom feeds
- Nested elements flattened
- Attributes prefixed with `@`

### CSV
- `text/csv`
- Standard comma-separated values
- Headers from first row

## Usage

### 1. Create a Database Connection

#### Option A: Dynamic URL Mode (Query-Level URL)

```sql
CREATE DATABASE my_api
WITH ENGINE = 'multi_format_api';
```

Then specify URL in each query:

```sql
SELECT * FROM my_api.data
WHERE url = 'https://api.example.com/data.json'
LIMIT 100;
```

#### Option B: Fixed URL Mode (Connection-Level URL)

```sql
CREATE DATABASE linkedin_jobs
WITH ENGINE = 'multi_format_api',
PARAMETERS = {
    "url": "https://api.talentify.io/linkedin-slots/feed"
};
```

Now queries don't need to specify URL:

```sql
-- URL is already configured, just query the data
SELECT title, company, location, date
FROM linkedin_jobs.data
LIMIT 100;
```

You can still override the default URL if needed:

```sql
SELECT * FROM linkedin_jobs.data
WHERE url = 'https://different-api.com/other-feed.xml'
LIMIT 50;
```

#### Option C: With Authentication Headers

```sql
CREATE DATABASE auth_api
WITH ENGINE = 'multi_format_api',
PARAMETERS = {
    "url": "https://api.example.com/data",
    "headers": {
        "Authorization": "Bearer YOUR_TOKEN",
        "X-API-Key": "your-key"
    },
    "timeout": 60
};
```

### 2. Query Data

#### Basic Query

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.example.com/data.json';
```

#### With Limit

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.example.com/feed.xml'
LIMIT 50;
```

#### With Custom Timeout

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.example.com/slow-endpoint.csv'
AND timeout = 60;
```

#### With Request-Specific Headers

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.example.com/data.json'
AND headers = '{"X-API-Key": "your-key"}';
```

## Real-World Examples

### Example 1: LinkedIn Slots XML Feed

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.talentify.io/linkedin-slots/feed'
LIMIT 100;
```

### Example 2: JSON API

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.github.com/repos/mindsdb/mindsdb'
LIMIT 1;
```

### Example 3: CSV Export

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://example.com/data/export.csv';
```

### Example 4: RSS Feed

```sql
SELECT title, link, pubDate
FROM my_api.data
WHERE url = 'https://news.ycombinator.com/rss'
LIMIT 20;
```

## Format Detection Logic

The handler detects the format in the following order:

1. **Content-Type Header**: Checks the HTTP response header
   - `application/json` → JSON parser
   - `application/xml` or `text/xml` → XML parser
   - `text/csv` → CSV parser

2. **URL Extension**: If Content-Type is ambiguous
   - `.json` → JSON parser
   - `.xml`, `feed`, `rss` → XML parser
   - `.csv` → CSV parser

3. **Content Analysis**: Inspects first character
   - `{` or `[` → JSON parser
   - `<` → XML parser

4. **Fallback**: Tries JSON, then XML

## Data Transformation

### JSON Handling

**Input:**
```json
{
  "users": [
    {"id": 1, "name": "Alice", "profile": {"age": 30}},
    {"id": 2, "name": "Bob", "profile": {"age": 25}}
  ]
}
```

**Output DataFrame:**
| id | name  | profile.age |
|----|-------|-------------|
| 1  | Alice | 30          |
| 2  | Bob   | 25          |

### XML Handling

**Input:**
```xml
<root>
  <item id="1">
    <name>Product A</name>
    <price>19.99</price>
  </item>
  <item id="2">
    <name>Product B</name>
    <price>29.99</price>
  </item>
</root>
```

**Output DataFrame:**
| @id | name      | price |
|-----|-----------|-------|
| 1   | Product A | 19.99 |
| 2   | Product B | 29.99 |

### CSV Handling

**Input:**
```csv
id,name,value
1,Item A,100
2,Item B,200
```

**Output DataFrame:**
| id | name   | value |
|----|--------|-------|
| 1  | Item A | 100   |
| 2  | Item B | 200   |

## Advanced Usage

### Joining with Other Tables

```sql
SELECT
    api_data.*,
    local_table.additional_info
FROM my_api.data AS api_data
JOIN local_table ON api_data.id = local_table.id
WHERE api_data.url = 'https://api.example.com/data.json';
```

### Using with Predictions

```sql
SELECT
    data.*,
    m.prediction
FROM my_api.data AS data
JOIN my_model AS m
WHERE data.url = 'https://api.example.com/data.json';
```

## Error Handling

### Common Errors

**Missing URL:**
```
Error: URL must be specified in WHERE clause
```
**Solution:** Always include `WHERE url = 'your-url'`

**Invalid Format:**
```
Error: Unable to detect or parse format for URL
```
**Solution:** Check that the URL returns valid JSON, XML, or CSV content

**Request Failed:**
```
Error: Failed to fetch data from URL: 404 Client Error
```
**Solution:** Verify the URL is accessible and returns a 200 status code

## Configuration Options

### Connection Parameters

| Parameter | Type   | Description                                    | Default |
|-----------|--------|------------------------------------------------|---------|
| headers   | dict   | Default headers for all requests               | {}      |

### Query Parameters

| Parameter | Type   | Description                                    | Default |
|-----------|--------|------------------------------------------------|---------|
| url       | string | URL to fetch data from (required)              | -       |
| headers   | string | JSON string of request-specific headers        | {}      |
| timeout   | int    | Request timeout in seconds                     | 30      |

## Limitations

1. **Dynamic Schemas**: Column schemas are determined at query time based on response content
2. **Memory**: Large responses are loaded entirely into memory
3. **Rate Limiting**: No built-in rate limiting (use external tools if needed)
4. **Authentication**: Only header-based auth (no OAuth flows)

## Dependencies

- `requests`: HTTP client
- `pandas`: Data manipulation
- `xml.etree.ElementTree`: XML parsing (Python stdlib)

## Troubleshooting

### Issue: Timeout Errors
**Solution:** Increase timeout parameter:
```sql
WHERE url = 'your-url' AND timeout = 60
```

### Issue: Authentication Required
**Solution:** Add authentication headers:
```sql
WHERE url = 'your-url'
AND headers = '{"Authorization": "Bearer YOUR_TOKEN"}'
```

### Issue: Nested Data Not Flattening
**Solution:** The handler automatically flattens nested structures. Check the actual column names returned - nested fields use dot notation (e.g., `profile.age`).

## Contributing

To add support for new formats:

1. Add format detection logic in `format_parsers.py::detect_format()`
2. Implement parser function (e.g., `parse_yaml()`)
3. Add format to `parse_response()` dispatcher
4. Update tests and documentation

## License

MIT License - see MindsDB main repository for details.

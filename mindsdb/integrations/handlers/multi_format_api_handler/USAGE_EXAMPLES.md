# Multi-Format API Handler - Usage Examples

## Quick Start

### 1. Create Database Connection

#### Dynamic URL Mode (Specify URL in Each Query)

```sql
CREATE DATABASE my_api
WITH ENGINE = 'multi_format_api';
```

#### Fixed URL Mode (Specify URL Once at Connection Level)

**Perfect for dedicated feeds like your LinkedIn Jobs API:**

```sql
CREATE DATABASE linkedin_jobs
WITH ENGINE = 'multi_format_api',
PARAMETERS = {
    "url": "https://api.talentify.io/linkedin-slots/feed"
};
```

Now you can query without specifying URL every time:

```sql
-- Simple and clean!
SELECT title, company, location, date
FROM linkedin_jobs.data
LIMIT 100;
```

## Tested Real-World Examples

### LinkedIn Slots XML Feed (Your Use Case!)

#### Method 1: Fixed URL (Recommended)

```sql
-- Setup once
CREATE DATABASE linkedin_jobs
WITH ENGINE = 'multi_format_api',
PARAMETERS = {
    "url": "https://api.talentify.io/linkedin-slots/feed"
};

-- Query anytime without URL
SELECT title, date, location, company, description
FROM linkedin_jobs.data
LIMIT 100;

-- Exclude large description field for better performance
SELECT title, company, location, date, jobType, experienceLevel
FROM linkedin_jobs.data
LIMIT 500;
```

#### Method 2: Dynamic URL

```sql
-- Specify URL in each query
SELECT title, date, location, company, description
FROM my_api.data
WHERE url = 'https://api.talentify.io/linkedin-slots/feed'
LIMIT 50;
```

**Returns columns:**
- title
- date
- applyUrl
- url
- partnerJobId
- referencenumber
- companyJobCode
- company
- companyId
- location
- country
- description
- jobFunctions
- industryCodes
- workplaceTypes
- jobType
- salaries
- experienceLevel

### JSON API Examples

#### GitHub Repository Info
```sql
SELECT name, full_name, description, stargazers_count, language
FROM my_api.data
WHERE url = 'https://api.github.com/repos/mindsdb/mindsdb';
```

#### JSONPlaceholder Users (with nested data)
```sql
SELECT name, email, username, phone, website,
       "address.city", "address.street", "address.zipcode"
FROM my_api.data
WHERE url = 'https://jsonplaceholder.typicode.com/users'
LIMIT 10;
```

**Note:** Nested JSON fields use dot notation (e.g., `address.city`)

#### JSONPlaceholder Posts
```sql
SELECT userId, id, title, body
FROM my_api.data
WHERE url = 'https://jsonplaceholder.typicode.com/posts'
LIMIT 20;
```

### CSV Examples

#### Iris Dataset
```sql
SELECT sepal_length, sepal_width, petal_length, petal_width, species
FROM my_api.data
WHERE url = 'https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv'
LIMIT 50;
```

## Advanced Usage

### With Custom Headers (Authentication)

```sql
-- Create database with default auth headers
CREATE DATABASE authenticated_api
WITH ENGINE = 'multi_format_api',
PARAMETERS = {
    "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE",
        "X-API-Key": "your-api-key"
    }
};

-- Then query normally
SELECT *
FROM authenticated_api.data
WHERE url = 'https://api.example.com/protected/data.json';
```

### Per-Request Headers

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://api.example.com/data.json'
AND headers = '{"X-Custom-Header": "value"}';
```

### Custom Timeout

```sql
SELECT *
FROM my_api.data
WHERE url = 'https://slow-api.example.com/data.xml'
AND timeout = 60;  -- 60 seconds timeout
```

### Joining with Other Tables

```sql
-- Join API data with local table
SELECT
    api.title,
    api.company,
    api.location,
    local.additional_info
FROM my_api.data AS api
JOIN local_jobs_table AS local
    ON api.partnerJobId = local.external_id
WHERE api.url = 'https://api.talentify.io/linkedin-slots/feed'
LIMIT 100;
```

### Using with AI Models

```sql
-- Analyze job descriptions with AI
SELECT
    title,
    company,
    description,
    m.sentiment,
    m.category
FROM my_api.data AS jobs
JOIN sentiment_model AS m
WHERE jobs.url = 'https://api.talentify.io/linkedin-slots/feed'
LIMIT 50;
```

## Format Detection Examples

The handler automatically detects the format:

| URL Pattern | Detected Format |
|-------------|-----------------|
| `*.json` or Content-Type: `application/json` | JSON |
| `*.xml` or Content-Type: `text/xml` | XML |
| `*.csv` or Content-Type: `text/csv` | CSV |
| `/feed`, `/rss` | XML (RSS/Atom) |
| Starts with `{` or `[` | JSON (fallback) |
| Starts with `<` | XML (fallback) |

## Testing the Handler

### Test Import
```python
from mindsdb.integrations.handlers.multi_format_api_handler.multi_format_api_handler import MultiFormatAPIHandler
handler = MultiFormatAPIHandler('test', connection_data={})
print(handler.check_connection())
```

### Test Format Detection
```python
from mindsdb.integrations.handlers.multi_format_api_handler.format_parsers import parse_response
import requests

response = requests.get('https://api.talentify.io/linkedin-slots/feed')
df = parse_response(response, 'https://api.talentify.io/linkedin-slots/feed')
print(f"Rows: {len(df)}, Columns: {list(df.columns)}")
```

## Common Issues & Solutions

### Issue: "URL must be specified in WHERE clause"
**Solution:** Always include the URL in your WHERE clause:
```sql
-- ✗ Wrong
SELECT * FROM my_api.data LIMIT 10;

-- ✓ Correct
SELECT * FROM my_api.data WHERE url = 'https://...' LIMIT 10;
```

### Issue: Columns not showing as expected
**Solution:** Check the actual returned columns - nested fields use dot notation:
```sql
-- To see all columns
SELECT * FROM my_api.data WHERE url = '...' LIMIT 1;
```

### Issue: Timeout errors
**Solution:** Increase timeout:
```sql
SELECT * FROM my_api.data
WHERE url = '...' AND timeout = 60;
```

## Supported Content Types

### JSON
- `application/json`
- `application/javascript`
- `text/json`

### XML
- `application/xml`
- `text/xml`
- RSS feeds
- Atom feeds

### CSV
- `text/csv`
- `text/plain` (if contains commas)

## Performance Tips

1. **Use LIMIT**: Always limit results for large datasets
2. **Select specific columns**: Don't use `SELECT *` if you only need a few columns
3. **Cache results**: Use MindsDB views to cache frequently accessed data
4. **Monitor timeouts**: Adjust timeout parameter for slow APIs

## Next Steps

- Check the [README.md](README.md) for detailed documentation
- See [format_parsers.py](format_parsers.py) for format detection logic
- Explore [multi_format_table.py](multi_format_table.py) for table implementation

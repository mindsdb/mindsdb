sql_description = """
MindsDB SQL is mostly compatible with DuckDB syntax.

- ONLY use tables, views, and predictors that appear in the Data Catalog provided to you. Never reference a table or model (e.g. mindsdb.sentiment_analyzer) that is not listed in the catalog—referencing a non-existent table causes "X not found. Available tables: [...]". If you need sentiment or other analysis, use only the tables from the catalog and express the logic in SQL.
- When writing the SQL query, make sure the select explicit names for the columns accordingly to the question.

Example:
SELECT movie_id, movie_description, age, name FROM someschema.movies WHERE whatever...;
Instead of:
SELECT * FROM somedb.movies WHERE whatever...;

- When composing JOIN queries, qualify every referenced column with its table (or table alias) (e.g., `movies.title`) so it is always clear which table provides each column.

- Date math & windows

Prefer simple interval arithmetic over dialect-specific functions. 
To subtract months: max_ts - INTERVAL 8 MONTH
To subtract days: max_ts - INTERVAL 30 DAY

Date types might be stored in string format, if you have error related to it (e.g. `No operator matches the given name and argument types`), use explicit type cast:  
CAST(max_ts AS TIMESTAMP)  - INTERVAL 30 DAY

Use DATE_TRUNC('month', timestamp_expression) for month bucketing.

- Monthly aggregation pattern

When asked for “last N months from the most recent date in column X”, follow this structure:

SELECT
    DATE_TRUNC('month', CAST(X AS TIMESTAMP)) AS month,
    COUNT(*) AS total
FROM <schema>.<table>
WHERE CAST(X AS TIMESTAMP) >= (
    SELECT DATE_TRUNC(
        'month',
        MAX(CAST(X AS TIMESTAMP)) - INTERVAL <N-1> MONTH
    )
    FROM <schema>.<table>
)
GROUP BY month
ORDER BY month;


Replace <N-1> with the number of months to go back excluding the current one (for “past 9 months including current”, use 8).

- COUNT and aggregates

Use COUNT(*) or COUNT(column). Never use COUNT() with empty parentheses.

If you change the SELECT list, update GROUP BY accordingly (or use GROUP BY 1, 2, ...).

- Error handling behavior

When you see an error like “function X does not exist”, do not try random alternative names (e.g., dateadd → DATE_ADD).

Instead, rewrite the logic using:

Intervals (ts - INTERVAL 8 MONTH), or

Simpler built-ins that you know are valid (e.g., just DATE_TRUNC with interval arithmetic).

- If an error says “requires 2 positional arguments, but 3 were provided”, remove the extra argument rather than reshuffling parameter order.
- If a MySQL function is not supported by MindsDB, try the DuckDB equivalent function.
- If you are unsure of the values of a possible categorical column, you can always write a query to explore the distinct values of that column to understand the data.
- If Metadata about a table is unknown, assume that all columns are of type varchar. 
- When casting varchars to something else simply use the CAST function, for example: CAST(year AS INTEGER), or CAST(year AS FLOAT), or CAST(year AS DATE), or CAST(year AS BOOLEAN), etc.
- ALWAYS: When writing queries that involve time, use the time functions in MindsDB SQL, or duckdb functions.
- ALWAYS: Include the name of the schema/database in query, for example, instead of `SELECT * FROM movies WHERE ...` write `SELECT * FROM somedb.movies WHERE ...`;
- ALWAYS: When columns contain spaces, special characters or are reserved words, use backticks (`) to quote the column name, for example, `column name` instead of [column name].
- `ILIKE` is only supported with some data sources; for portable case-insensitive matching use LOWER(column) LIKE LOWER('%pattern%') instead of column ILIKE '%pattern%'.
"""

sql_with_kb_description = """

MindsDB SQL is compatible with MySQL and DuckDB syntax, with additional features for knowledge bases.

When the question requires to filter by something semantically, use the knowledge bases available when possible.
You can determine what knowledge bases are relevant given the data catalog.

Example:
Knowledge Base Metadata:
kb,kb_insert_query,parent_query_id_column,parent_query_content_columns,parent_query_metadata_columns
mindsdb.somekb,"INSERT INTO somekb SELECT movie_id AS `id`, movie_description AS content FROM somedb.movies",id,content,age, name

This tells you that somekb is a knowledge base that was created from the movies table 
and that you can use it so search information about the movies descriptions, which is better than 
trying to do keyword search on the movies table. For that here is a detailed description of how to use knowledge bases.

**Knowledge Base Queries:**
Knowledge bases are semantic search tables that allow you to find relevant entries using semantic text search and metadata filtering.

Example queries:
- Semantic search:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND relevance >= 0.5`
- Metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE metadata_column = 'value'`
- Combined semantic and metadata filtering:  
  `SELECT * FROM mindsdb.kb_name WHERE content LIKE 'your semantic search query' AND metadata_column = 'value' AND relevance >= 0.5`

Where output columns will be: id,chunk_id,chunk_content,<one or more metadata columns>,relevance,distance
use `relevance > 0.5` to filter for relevant results.

From the knowledge base, you can identify where id came from, and what content comes from, so when you SELECT, you can rename columns accordingly.

For example, to find up to 10,000 movies that are excellent, not horror, and have an age group of PG-13 or higher (assuming a metadata column named "age"):
instead of searching the movies table semantically SELECT * FROM somedb.movies WHERE age >= 13 AND description LIKE '%excellent%' LIMIT 10000;
which is prone to missing results as it is likely to miss results where people write similar things to excellent and horror, but in a different way.
as such, you can search the knowledge base, which does not require any exact matches, it will filter by most relevant results. 
```
SELECT 
   id as movie_id,
   chunk_id as movie_description_chunk_id,
   chunk_content as movie_description_chunk,
   age as rated_age, -- assuming age is a metadata column
   name as movie_name, -- assuming name is a metadata column
   relevance
FROM mindsdb.movies_kb
WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5
LIMIT 10000;
```


or since there can be multiple chunks for a movie that match the query you can aggregate results:

```
SELECT
   id AS movie_id,
   LIST(chunk_id) AS movie_description_chunk_ids,
   LIST(chunk_content) AS movie_description_chunks,
   MAX(relevance) AS max_relevance,  -- the most relevant chunk
   age AS rated_age, -- assuming age is a metadata column, same for each id
   name AS movie_name -- assuming name is a metadata column, same for each id
FROM mindsdb.movies_kb
WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5
GROUP BY id, age, name
ORDER BY max_relevance DESC
LIMIT 10000;
```


Suppose you have a table `movies_data` from which the knowledge base was created. To answer: "Show the count of excellent, non-horror, PG-13+ movies by release year":

```
SELECT release_year, COUNT(*) FROM db.movies_data
WHERE movie_id IN (
    SELECT DISTINCT id FROM mindsdb.movies_kb
    WHERE content LIKE 'excellent, not horror' AND age >= 13 AND relevance >= 0.5 LIMIT 10000
)
GROUP BY release_year;
```

- NEVER use `ILIKE` in knowledge base `content` condition  (ILIKE is not supported in knowledge bases) only LIKE is supported.

- AVOID direct joins between tables and knowledge bases. Instead, use `WHERE <IDCOLUMN> IN (SELECT DISTINCT id FROM knowledge_base) ...`
OR use the knowledge base as a subquery and join on that, for example:
`SELECT * FROM <TABLE> JOIN (SELECT id, LIST(chunk_content) FROM knowledge_base WHERE content LIKE 'your semantic search query' AND metadata_col=something ... GROUP BY id LIMIT 10000 ) AS kb ON <TABLE>.<IDCOLUMN> = kb.id ...`

- ALWAYS: It is important to set an appropriate LIMIT on knowledge base queries to avoid missing results; the default limit is 10, so if you need more than 10, set it accordingly. When unsure LIMIT 10000 is recommended.

- When writing the SQL query, make sure the select renames the columns accordingly to the question.

"""

markdown_instructions = """
**IMPORTANT FORMATTING REQUIREMENTS:**
- Always format your responses in Markdown
- When presenting tabular data or query results, organize them as Markdown tables
- Use proper Markdown table syntax with headers and aligned columns
- For example:
| Column1 | Column2 | Column3 |
|---------|---------|---------|
| Value1  | Value2  | Value3  |
- Use other Markdown formatting (headers, lists, code blocks) as appropriate to make responses clear and well-structured
"""

planning_prompt_base = """
Before writing any SQL queries, create a plan for how to solve the question.

The plan should:
1. Identify what data sources (tables or knowledge bases) are relevant to answer the question
1.1 When referring to tables or knowledge bases in the plan. Always include the name of the schema/database in the plan along with the table name. For example: database_name.table_name or database_name.knowledge_base_name
2. Outline the steps needed to solve the question, each step may correspond to some exploration query that you may need to do, describe the exploratory step, but do not write the query.
2.1 {response_instruction}
2.2 Note: exploratory steps, can be for example:
- if we can see that we will filter by the value of one column that is categorical we may need to first explore the DISTINCT values of that column to understand what values to filter by in WHERE col=<something>
3. Specify what information might need to be explored or collected
4. Keep the number of steps to a minimum (try to solve with as few steps as possible)
5. Maximum number of steps should not exceed 

Do NOT write any SQL queries in the plan. Just describe:
- What data you will use
- What steps you will take
- What information you might need to explore first

Example plan format:
Step 1: Sample table X to understand available columns and data
Step 2: Check distinct values in column Y to understand the data
Step 3: Query table X with filters based on the question requirements
Step 4: Run a test query with a LIMIT 10 to make sure the query is working as expected
Step 5: Aggregate results as needed

Keep steps concise and focused on solving the question efficiently.
"""

chart_generation_prompt = """
You are an expert at generating Chart.js configurations from SQL queries. Your task is to:

IMPORTANT CONTEXT - HOW THE CHART WILL BE USED:
The chart configuration you generate will be used directly in the frontend with Chart.js. The frontend code will be:
```javascript
const ctx = document.getElementById('myChart');
const chart = new Chart(ctx, chartConfig);
```
Where `chartConfig` is the exact dictionary you generate in the `chartjs_config` field. 

This means:
- Your configuration must be a complete, valid Chart.js configuration object
- It must be compatible with Chart.js v3+ API
- It will be passed directly to `new Chart(ctx, chartConfig)` without any modifications
- The configuration must include all required fields (especially 'type') and be ready for immediate use

1. Analyze the provided SQL query to understand the data structure and relationships
2. Determine the most appropriate chart type from: 'line', 'bar', 'pie', or 'doughnut'
3. Generate a Chart.js configuration dictionary with the following REQUIRED structure:
   - `type`: REQUIRED - MUST always be included. One of 'line', 'bar', 'pie', or 'doughnut'. This field is MANDATORY and must never be omitted.
   - `options`: Chart.js options object (e.g., responsive, plugins.title, scales for line/bar charts)
   - `labels`: Empty array [] (will be populated from the first column of query results)
   - `datasets`: Array of dataset objects, each with:
     - `label`: The column name (from the data query, excluding the first column)
     - `data`: Empty array [] (will be populated programmatically from query results)
     - Additional dataset-specific properties (e.g., `backgroundColor`, `borderColor` for line/bar charts)
   
   IMPORTANT: The 'type' field is REQUIRED and MUST always be present in the chartjs_config. You MUST include it in every response. Choose the most appropriate chart type for the data unless the user explicitly asks for a different chart type.
   - Make sure you specify if needed the axis scales and types

4. Generate a data transformation SQL query string with the following format:
   SELECT labels, <dataset_col1>, <dataset_col2>, ... 
   FROM (
       <select TRANSFORMATION QUERY> from (<ORIGINAL QUERY>) <where filters plus other transformations/aggregations,limits, etc>
   )

CRITICAL - COLUMNS AVAILABLE FOR THE DATA QUERY:
The data query runs on a dataframe "df" that contains ONLY the columns from the Sample Data Catalog (the columns returned by the original query). You MUST NOT reference any column that is not listed in "Column Analysis" / the catalog. Referencing a missing column causes: Binder Error "Referenced column X not found in FROM clause! Candidate bindings: Y".
Example: If the catalog only lists "answer", then "question" does NOT exist in df. WRONG: SELECT answer FROM df WHERE question = '...' (uses "question", which is not in df). RIGHT: SELECT answer AS labels, count(*) AS value FROM df GROUP BY answer (only uses "answer").

Guidelines:
- The first column in the data query should be named 'labels' (this will be used for x-axis labels or pie chart labels)
- Subsequent columns should be named descriptively and will become dataset labels
- For line and bar charts: First column = x-axis labels, other columns = y-axis data series
- For pie and doughnut charts: First column = labels, second column = values (single dataset)
- Apply appropriate transformations, aggregations, and filters to the original query as needed
- Include ORDER BY clauses when appropriate (e.g., for time series data). When ordering by a column alias that contains spaces or special characters, use backticks (MindsDB uses MySQL-like quoting): ORDER BY `Column Alias` DESC (never single quotes).
- The data query should be valid MindsDB SQL that can be executed directly

Example for a line chart:
If the original query is: SELECT date, sales FROM db.orders
The data query might be: 
SELECT date AS labels, sales FROM (SELECT date, SUM(amount) AS sales FROM db.orders GROUP BY date ORDER BY date)

Example for a pie chart:
If the original query is: SELECT category, COUNT(*) FROM db.products
The data query might be:
SELECT category AS labels, COUNT(*) AS value FROM (SELECT category, COUNT(*) FROM db.products GROUP BY category)

Example with column alias containing spaces (use backticks in ORDER BY):
If the original query is: SELECT product_name, count(*) AS `Number of Reviews` FROM df GROUP BY product_name
The data query must use backticks in ORDER BY: SELECT product_name AS labels, count(*) AS `Number of Reviews` FROM df GROUP BY product_name ORDER BY `Number of Reviews` DESC LIMIT 10
Never write ORDER BY 'Number of Reviews' (single quotes)—that is a string literal and will fail in DuckDB.

Remember:
- Chart.js config MUST ALWAYS include the 'type' field - this is REQUIRED and cannot be omitted
- Chart.js config should have empty arrays for labels and datasets[].data
- The data_query_string should be a complete, executable SQL query
- Only reference columns that appear in the Sample Data Catalog; never use columns (e.g. "question") that are not in the catalog—df only has the columns from the original query result
- Choose chart types appropriately: line/bar for time series or comparisons, pie/doughnut for proportions
- The 'type' field must be one of: 'line', 'bar', 'pie', or 'doughnut' - always include it in your response
"""

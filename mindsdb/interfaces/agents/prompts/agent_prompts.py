sql_description = """
MindsDB SQL is mostly compatible with DuckDB syntax.

- When writing the SQL query, make sure the select explicit names for the columns accordingly to the question.

Example:
SELECT movie_id, movie_description, age, name FROM someschema.movies WHERE whatever...;
Instead of:
SELECT * FROM somedb.movies WHERE whatever...;


- Date math & windows

Prefer simple interval arithmetic over dialect-specific functions.

To subtract months:
max_ts - INTERVAL 8 MONTH

To subtract days:
max_ts - INTERVAL 30 DAY

Use DATE_TRUNC('month', timestamp_expression) for month bucketing.

Avoid using DATEADD, DATE_ADD, or other guessed function names unless they already appear in a working example for this connection.

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

When you see an error like “function X does not exist”, do not try random alternative names (e.g., dateadd → DATE_ADD → DATE_ADDD).

Instead, rewrite the logic using:

Intervals (ts - INTERVAL 8 MONTH), or

Simpler built-ins that you know are valid (e.g., just DATE_TRUNC with interval arithmetic).

- If an error says “requires 2 positional arguments, but 3 were provided”, remove the extra argument rather than reshuffling parameter order.
- If a MySQL function is not supported by MindsDB, try the DuckDB equivalent function.
- If you are unsusre of the values of a possible categorical column, you can always write a query to explore the distinct values of that column to understand the data.
- If Metadata about a table is unknown, assume that all columns are of type varchar. 
- When casting varchars to something else simply use the CAST function, for example: CAST(year AS INTEGER), or CAST(year AS FLOAT), or CAST(year AS DATE), or CAST(year AS BOOLEAN), etc.
- When a column has been casted and renamed, the new name can and should be used in the query, for example:
do:
SELECT CAST(datetime AS DATE) as ndate FROM somedb.movies WHERE ndate >= something
instead of:
SELECT CAST(datetime AS DATE) as ndate FROM somedb.movies WHERE CAST(datetime AS DATE) >= something

 if you cast the column year CAST(year AS INTEGER) AS year_int, you can use year_int in the query such as WHERE year_int > 2000. 
- ALWAYS: When writing queries that involve time, use the time functions in MindsDB SQL, or duckdb functions.
- ALWAYS:Include the name of the schema/database in query, for example, instead of `SELECT * FROM movies WHERE ...` write `SELECT * FROM somedb.movies WHERE..`;
- ALWAYS: When columns contain spaces, special characters or are reserved words, use double quotes `"` to quote the column name, for example, "column name" instead of [column name].
"""

sql_with_kb_description = """

MindsDB SQL is compatible with MySQL and DuckDB syntax, with additional features for knowledge bases.

When the question requires to filter by something semantically, use the knowledge bases available when possible.
You can determine what knowledge bases are relevant given teh data catalog.

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
use relevance >0.5 to filter for relevant results.

From the knowledge base, you can identify where id came from, and what content comes from, so when you SELECT, you can rename columns accordingly.

For example, to find up to 10,000 movies that are excellent, not horror, and have an age group of PG-13 or higher (assuming a metadata column named "age"):
instead of searching the movies table seamantically SELECT * FROM somedb.movies WHERE age >= 13 AND description LIKE '%excellent%' LIMIT 10000;
which is prone to missing results as it is likely to miss results where people write similar things to exellent and horror, but in a different way.
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

planning_prompt = """
Before writing any SQL queries, create a plan for how to solve the question.

The plan should:
1. Identify what data sources (tables or knowledge bases) are relevant to answer the question
1.1 When referring to tables or knowledge bases in the plan. Always include the name of the schema/database in the plan along with the table name. For example: database_name.table_name or database_name.knowledge_base_name
2. Outline the steps needed to solve the question, each step may correspond to some exploration query that you may need to do, describe the exploratory step, but do not write the query.
2.1 If there is no need for exploratorys steps, describe how would you write the final query. and that you can solve this in one step.
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
      
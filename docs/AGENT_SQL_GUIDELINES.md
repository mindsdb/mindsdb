Always Use MindsDB SQL Dialect to write queries.

Most MySQL queries will work in MindsDB.

# Some Important exceptions, or not supported statements:

- ROLLUP statements: When doing aggregates, MindsDB does not currently support ROLLUP statements, find an alternative way to group.
- Common Table Expressions (CTE)


# MindsDB SQL guidelines for specific question types:

- Date math & windows

MindsDB, prefers simple interval arithmetic over dialect-specific functions.

To subtract months:
max_ts - INTERVAL 8 MONTH

To subtract days:
max_ts - INTERVAL 30 DAY

- Date aggregations: 
  - Always use DATE_TRUNC for Monthly, daily, .. etc, 
  - Avoid using DATEADD, DATE_ADD, or other guessed function names unless they already appear in a working example.


Examples: 
Use DATE_TRUNC('month', timestamp_expression) for month bucketing.

When asked for "last N months from the most recent date in column X", follow this structure:

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


Replace <N-1> with the number of months to go back excluding the current one (for "past 9 months including current", use 8).

- COUNT and aggregates

Use COUNT(*) or COUNT(column). Never use COUNT() with empty parentheses.

If you change the SELECT list, update GROUP BY accordingly (or use GROUP BY 1, 2, ...).

- CASTING:

    - When casting varchars to something else simply use the CAST function, for example: CAST(year AS INTEGER), or CAST(year AS FLOAT), or CAST(year AS DATE), or CAST(year AS BOOLEAN), etc.
    - When a column has been casted and renamed, the new name can and should be used in the query, for example:
    do:
SELECT CAST(datetime AS DATE) as ndate FROM somedb.movies WHERE ndate >= something
    instead of:
SELECT CAST(datetime AS DATE) as ndate FROM somedb.movies WHERE CAST(datetime AS DATE) >= something

    - if you cast the column year CAST(year AS INTEGER) AS year_int, you can use year_int in the query such as:
    
SELECT CAST(year AS INTEGER) AS year_int FROM somedb.movies WHERE year_int > 2000. 



# Error handling behavior:

    - If you run into a funciton error, try the DuckDB/Postgres equivalent. Do not try unknown/random alternative names. Instead, rewrite the logic the Simplest built-ins that you know are valid in Mysql or DuckDB. even if the query is longer that way.
    - If an error says "requires 2 positional arguments, but 3 were provided", remove the extra argument rather than reshuffling parameter order.
    - If you are unsusre of the values of a possible categorical column, you can always write a query to explore the distinct values of that column to understand the data.
    - If Metadata about a table is unknown, assume that all columns are of type varchar. 



# Query requirements

- When writing the SQL query, make sure the select explicit names for the columns accordingly to the question.

Example:
SELECT movie_id, movie_description, age, name FROM someschema.movies WHERE whatever...;
Instead of:
SELECT * FROM somedb.movies WHERE whatever...;


- ALWAYS:Include the name of the schema/database in query, for example, instead of `SELECT * FROM movies WHERE ...` write `SELECT * FROM somedb.movies WHERE..`;
- ALWAYS: When columns contain spaces, special characters or are reserved words, use double quotes `"` to quote the column name, for example, "column name$" instead of simply: column name$.



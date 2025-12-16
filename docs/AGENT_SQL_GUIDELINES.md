# MindsDB SQL Guidelines

Always use the MindsDB SQL dialect to write queries.

Most MySQL queries will work in MindsDB, however there are some constraints and preferences, that you must follow.



## Not Supported Statements in MindsDB SQL

- **ROLLUP statements**: For aggregates, MindsDB supports standard GROUP BY statements, however,  MindsDB does not currently support statements with ROLLUP. Find an alternative way to do those using standard GROUP BY operations.
- **Common Table Expressions (CTE)**: CTEs are not supported.

---

# Preferred MindsDB SQL grammar

MindsDB queries for certain types of operations preffers certain syntax and SQL grammar and constraints.

## Date Math and Windows

MindsDB prefers simple interval arithmetic over dialect-specific functions.

### Subtracting Months
```sql
max_ts - INTERVAL 8 MONTH
```

### Subtracting Days
```sql
max_ts - INTERVAL 30 DAY
```

---

## Date Aggregations

- Always use `DATE_TRUNC` for monthly, daily, etc. aggregations.
- Avoid using `DATEADD`, `DATE_ADD`, or other guessed function names unless they already appear in a working example.

### Examples

Use `DATE_TRUNC('month', timestamp_expression)` for month bucketing.

When asked for "last N months from the most recent date in column X", follow this structure:

```sql
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
```

**Note**: Replace `<N-1>` with the number of months to go back, excluding the current one. For example, for "past 9 months including current", use 8.

---

## COUNT and Aggregates

- Use `COUNT(*)` or `COUNT(column)`. Never use `COUNT()` with empty parentheses.
- If you change the SELECT list, update GROUP BY accordingly (or use `GROUP BY 1, 2, ...`).

---

## Casting

### Basic Casting
When casting VARCHARs to other types, use the `CAST` function:

- `CAST(year AS INTEGER)`
- `CAST(year AS FLOAT)`
- `CAST(year AS DATE)`
- `CAST(year AS BOOLEAN)`

### Using Casted Column Names
When a column has been cast and renamed, use the new name in the query.

**Do:**
```sql
SELECT CAST(datetime AS DATE) AS ndate 
FROM somedb.movies 
WHERE ndate >= something;
```

**Instead of:**
```sql
SELECT CAST(datetime AS DATE) AS ndate 
FROM somedb.movies 
WHERE CAST(datetime AS DATE) >= something;
```

### Example with Renamed Cast
If you cast a column as `CAST(year AS INTEGER) AS year_int`, you can use `year_int` in the query:

```sql
SELECT CAST(year AS INTEGER) AS year_int 
FROM somedb.movies 
WHERE year_int > 2000;
```


## Query Grammar Requirements

### Explicit Column Names
When writing SQL queries, make sure the SELECT statement uses explicit column names according to the question.

**Example:**
```sql
SELECT movie_id, movie_description, age, name 
FROM someschema.movies 
WHERE whatever...;
```

**Instead of:**
```sql
SELECT * 
FROM somedb.movies 
WHERE whatever...;
```

### Schema/Database Names
**ALWAYS**: Include the name of the schema/database in queries. For example, instead of `SELECT * FROM movies WHERE ...`, write `SELECT * FROM somedb.movies WHERE ...`.

### Column Names with Special Characters
**ALWAYS**: When columns contain spaces, special characters, or are reserved words, use double quotes `"` to quote the column name. For example, use `"column name$"` instead of `column name$`.


---

# Error Handling Behavior

- If you run into a function error, try the DuckDB/Postgres equivalent. Do not try unknown or random alternative names. Instead, rewrite the logic using the simplest built-ins that you know are valid in MySQL or DuckDB, even if the query is longer that way.
- If an error says "requires 2 positional arguments, but 3 were provided", remove the extra argument rather than reshuffling parameter order.
- If you are unsure of the values of a possible categorical column, you can always write a query to explore the distinct values of that column to understand the data.
- If metadata about a table is unknown, assume that all columns are of type VARCHAR.

---

# Style Guide for MindsDB Documentation

## Syntax for SQL commands

Follow the rules below when writing an SQL command.

* Add a semi-colon `;` at the end of each SQL command.
* Use all-caps when writing the keywords, such as `SELECT`, `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `PREDICT`, `AS`, `CREATE TABLE`, `INSERT INTO`, etc.
* When writing a query, start a new line for the following keywords: `SELECT`, `FROM`, `JOIN`, `WHERE`, `GROUP BY`, `ORDER BY`, `PREDICT`, `USING`, `AND`, `OR`. It is to avoid the horizontal scrollbar.

### Example

```sql
SELECT *
FROM table_name_1 a
JOIN table_name_2 b
WHERE column_name_1=value_name
GROUP BY a.column_name_2
ORDER BY b.column_name_1;
```

## Syntax for SQL commands along with their output

Follow the syntax below when documenting an SQL command and its output.

    ```sql
    QUERY GOES HERE
    ```

    On execution, we get:

    ```
    +---------------+---------------+
    | [column_name] | [column_name] |
    +---------------+---------------+
    | [value]       | [value]       |
    +---------------+---------------+
    ```

    Where:

    | Name                                | Description                         |
    | ----------------------------------- | ----------------------------------- |
    | `VARIABLE NAME GOES HERE`           | VARIABLE DESCRIPTION GOES HERE      |

!!! note
    If the output is not a table, remove the output table from above and place your output there.

### Example 1

    ```sql
    SELECT *
    FROM table_name_1 a
    JOIN table_name_2 b
    WHERE column_name=value_name;
    ```

    On execution, we get:

    ```
    +---------------+---------------+
    | [column_name] | [column_name] |
    +---------------+---------------+
    | [value]       | [value]       |
    +---------------+---------------+
    ```

    Where:

    | Name                                | Description                 |
    | ----------------------------------- | --------------------------- |
    | `column_name`                       | column description          |

### Example 2

    ```sql
    CREATE PREDICTOR mindsdb.predictor_name
    FROM integration_name
        (SELECT column_name FROM table_name)
    PREDICT target_column;
    ```

    On execution, we get:

    ```
    OUTPUT GOES HERE
    ```

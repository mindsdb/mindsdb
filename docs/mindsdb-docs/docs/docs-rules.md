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
WHERE column_name_1=value_name_1
AND column_name_2=value_name_2
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
    If the output is not a table, remove the output table from above and place your output message there.

!!! note
    Please note that the **```sql** block is used only in the case of SQL commands/queries. For the output table/message, we use a standard block. Whereas, for the description table, we use just a table. See the examples below.

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

#### Output of Example 1

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
        (SELECT column_name_1, column_name_2, target_column FROM table_name)
    PREDICT target_column;
    ```

    On execution, we get:

    ```
    OUTPUT GOES HERE
    ```

#### Output of Example 2

```sql
CREATE PREDICTOR mindsdb.predictor_name
FROM integration_name
    (SELECT column_name_1, column_name_2, target_column FROM table_name)
PREDICT target_column;
```

On execution, we get:

```
OUTPUT GOES HERE
```

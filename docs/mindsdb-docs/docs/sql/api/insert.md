# INSERT INTO Statement

## Description

The `#!sql INSERT INTO` statement is used to insert data into a table. This inserted data comes from the subselect statement. It is commonly used to input predictions into the database.

## Syntax

```sql
INSERT INTO [integration_name].[table_name]
    (SELECT ...);
```

It performs a subselect `#!sql (SELECT ...)` and gets data from there. Then, this data is inserted into a table of integration `#!sql [integration_name]` using `#!sql INSERT INTO TABLE [table_name]` statement.

On execution, we get:

```sql
Query OK, 0 row(s) updated - x.xxxs
```

## Example

In this example, we want to input the predictions into a table `#!sql int1.tbl1`. Given is the following schema:

```bash
int1
└── tbl1
mindsdb
└── predictor_name
int2
└── tbl2
```

Where:

|                  | Description                                                 |
| ---------------- | ----------------------------------------------------------- |
| `int1`           | Integration name where the table `tbl1` resides             |
| `tbl1`           | Table where data will be inserted                           |
| `predictor_name` | Name of the ML model                                        |
| `int2`           | Database to be used as a source in the inner `#!sql SELECT` |
| `tbl2`           | Table to be used as a source                                |

In order to achieve the desired result, we execute the following query:

```sql
INSERT INTO int1.tbl1 (
    SELECT *
    FROM int2.tbl2 AS ta
    JOIN mindsdb.predictor_name AS tb
    WHERE ta.date > '2015-12-31'
);
```

On execution, we get:

```sql
Query OK, 0 row(s) updated - x.xxxs
```

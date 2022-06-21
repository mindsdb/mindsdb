# INSERT INTO Statement

## Description

The `#!sql INSERT INTO` statement is used to fill a table with the result of subselect. commonly used to persist predictions into the database

## Syntax

```sql
    INSERT INTO [integration_name].[table_name]
    [SELECT ...]
```

It performs a subselect `#!sql [SELECT ...]` and gets data from it there after it performs `#!sql INSERT INTO TABLE [table_name]` of integration [integration_name]

## Example

In this example we want to persist the predictions into a table `#!sql int1.tbl1`. Given the following schema:

```bash
int1
└── tbl1
mindsdb
└── predictor_name
int2
└── tbl2
```

Where:

|                  | Description                                                |
| ---------------- | ---------------------------------------------------------- |
| `int1`           | Integration for the table to be created in                 |
| `tbl1`           | Table to be created                                        |
| `predictor_name` | Name of the model to be used                               |
| `int2`           | Database to be used as a source in the inner `#!sql SELECT` |
| `tbl2`           | Table to be used as a source.                               |

In order to achive the desired result we could execute the following query:

```sql
INSERT INTO int1.tbl1 (
    SELECT *
    FROM int2.tbl2 AS ta
    JOIN mindsdb.predictor_name AS tb
    WHERE ta.date > '2015-12-31'
)
```

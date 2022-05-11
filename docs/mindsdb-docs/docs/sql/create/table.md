# CREATE TABLE Statement

The `#!sql CREATE TABLE` statement is used to create table and fill it with result of a subselect.

## Syntax

```sql
    CREATE /*REPLACE*/ TABLE [integration_name].[table_name]
    [SELECT ...]
```

It performs a subselect `#!sql [SELECT ...]` and gets data from it, thereafter it creates a table `#!sql [table_name]` in  `#!sql [integration_name]`. lastly it performs an `#!sql INSERT INTO [integration_name].[table_name]` with the contents of the `#!sql [SELECT ...]`

!!!warning "`#!sql REPLACE`"
    If `#!sql REPLACE` is indicated then `#!sql [integration_name].[table_name]` will be  **Dropped**


## Example

In this example we want to persist the predictions into a table `#!sql int1.tbl1`. Given the following schema:

```bash
int1
└── tbl1
mindsdb
└── predictor
int2
└── tbl2
```
Where:

|                     | Description                                  |
| ------------------- | -------------------------------------------- |
| `int1`              | Integration for the table to be created in |
| `tbl1`            | Table to be created   |
| `predictor`           | Name of the `#!sql PREDICTOR`    |
| `int2`          | Database to be used a a source in the inner `#!sql SELECT` |
| `tbl2`     | Table to be used a a source. |

In order to achive the desired result we could execute the following query:

```sql
CREATE TABLE int1.tbl1 
SELECT * FROM (
    SELECT * FROM int2.fish AS ta                  
    WHERE ta.date > '2015-12-31'
)
JOIN mindsdb.tp3 AS tb 
```

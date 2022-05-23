# `#!sql DROP PREDICTOR` Statement

## Description

The `#!sql DROP PREDICTOR` statement is used to delete the model table:

## Syntax

```sql
DROP PREDICTOR [predictor_name];
```

On execution:

```sql
Query OK, 0 rows affected (0.058 sec)
```

Where:

|                    | Description                     |
| ------------------ | ------------------------------- |
| `[predictor_name]` | Name of the model to be deleted |

## Validation

```sql
SELECT name FROM mindsdb.predictors
    WHERE name = '[predictor_name]';
```

On execution:

```sql
Empty set (0.026 sec)
```

## Example

The following SQL statement drops the model table called `home_rentals_model`. Given the followig query to list all predictors by name

```sql
SELECT name FROM mindsdb.predictors;
```

Resulting in a table with 2 rows:

```sql
+---------------------+
| name                |
+---------------------+
| other_model         |
+---------------------+
| home_rentals_model  |
+---------------------+
```

Execute the `#!sql DROP PREDICTOR` statement as:

```sql
DROP PREDICTOR home_rentals_model;
```

On execution:

```sql
Query OK, 0 rows affected (0.058 sec)
```

Validate that the model has been deleted by listing again all predictors by name:

```sql
SELECT name FROM mindsdb.predictors;
```

Resulting in a table with only one row, and without the dropped model:

```sql
+---------------------+
| name                |
+---------------------+
| other_model         |
+---------------------+
```

# `#!sql RETRAIN` Statement

## Description

The `RETRAIN` statement is used to retrain old predictors.
The predictor is updated to leverage any new data in optimizing its predictive capabilities, without necessarily taking as long to train as starting from scratch. The basic syntax for retraining the predictors is:

## Syntax

```sql
RETRAIN mindsdb.[predictor_name];
```

On execution:

```sql
Query OK, 0 rows affected (0.058 sec)
```
## Validation

```sql
SELECT name, update_status FROM mindsdb.predictors
    WHERE name = '[predictor_name]';
```

On execution:

```sql
+------------------+---------------+
| name             | update_status |
+------------------+---------------+
| [predictor_name] | up_to_date    |
+------------------+---------------+
```

Where:

|                    | Description                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------- |
| `[predictor_name]` | Name of the model to be retrained                                                        |
| `update_status`    | Column from `#!sql mindsdb.predictors` that informs if the model can be retrained or not |

## Example

### Validating Prior Status

```sql
SELECT name, update_status FROM mindsdb.predictors
    WHERE name = 'home_rentals_model';
```

On execution:

```sql
+--------------------+---------------+
| name               | update_status |
+--------------------+---------------+
| home_rentals_model | available     |
+--------------------+---------------+
```

Note the value for `update_status` is `available`

### Retraining model

```sql
RETRAIN home_rentals_model;
```

On execution:

```sql
Query OK, 0 rows affected (0.058 sec)
```

### Validating Resulting Status

```sql
SELECT  name, update_status FROM mindsdb.predictors
    WHERE name = 'home_rentals_model';
```

On execution:

```sql
+--------------------+---------------+
| name               | update_status |
+--------------------+---------------+
| home_rentals_model | up_to_date    |
+--------------------+---------------+
```

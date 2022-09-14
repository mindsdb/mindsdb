# `#!sql RETRAIN` Statement

## Description

The `#!sql RETRAIN` statement is used to retrain the already trained predictors with the new data.

The predictor is updated to leverage the new data in optimizing its predictive capabilities. Retraining takes less time than training the predictor from scratch, so it is useful when the new training data is available.

## Syntax

Here is the syntax:

```sql
RETRAIN mindsdb.[predictor_name];
```

On execution, we get:

```sql
Query OK, 0 rows affected (0.058 sec)
```

## When to `#!sql RETRAIN` the Model?

To find out whether you need to retrain your model, query the `mindsdb.predictors` table and look for the `update_status` column.

Here are the possible values of the `update_status` column:

| Name          | Description                                                                                                 |
| ------------- | ----------------------------------------------------------------------------------------------------------- |
| `available`   | It indicates that the new data for retraining the model is available, and the model should be updated.      |
| `updating`    | It indicates that the retraining process of the model takes place.                                          |
| `up_to_date`  | It indicates that your model is up to date and does not need to be retrained.                               |

Let's run the query.

```sql
SELECT name, update_status
FROM mindsdb.predictors
WHERE name = '[predictor_name]';
```

On execution, we get:

```sql
+------------------+---------------+
| name             | update_status |
+------------------+---------------+
| [predictor_name] | up_to_date    |
+------------------+---------------+
```

Where:

| Name               | Description                                                  |
| ------------------ | ------------------------------------------------------------ |
| `[predictor_name]` | Name of the model to be retrained.                           |
| `update_status`    | Column informing whether the model needs to be retrained.    |

## Example

Let's look at an example using the `home_rentals_model` model.

First, we check the status of the predictor.

```sql
SELECT name, update_status
FROM mindsdb.predictors
WHERE name = 'home_rentals_model';
```

On execution, we get:

```sql
+--------------------+---------------+
| name               | update_status |
+--------------------+---------------+
| home_rentals_model | available     |
+--------------------+---------------+
```

The `available` value of the `update_status` column informs us that the new data is available, and we can retrain the model.

```sql
RETRAIN mindsdb.home_rentals_model;
```

On execution, we get:

```sql
Query OK, 0 rows affected (0.058 sec)
```

Now, let's check the status again.

```sql
SELECT  name, update_status
FROM mindsdb.predictors
WHERE name = 'home_rentals_model';
```

On execution, we get:

```sql
+--------------------+---------------+
| name               | update_status |
+--------------------+---------------+
| home_rentals_model | updating      |
+--------------------+---------------+
```

And after the retraining process is completed:

```sql
SELECT  name, update_status
FROM mindsdb.predictors
WHERE name = 'home_rentals_model';
```

On execution, we get:

```sql
+--------------------+---------------+
| name               | update_status |
+--------------------+---------------+
| home_rentals_model | up_to_date    |
+--------------------+---------------+
```

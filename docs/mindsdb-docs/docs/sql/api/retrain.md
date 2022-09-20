# `#!sql RETRAIN` Statement

## Description

The `#!sql RETRAIN` statement is used to retrain the already trained predictors with the new data. The predictor is updated to leverage the new data in optimizing its predictive capabilities.

Retraining takes at least as much time as the training process of the predictor did because now the dataset used to retrain has new or updated data.

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

It is advised to `RETRAIN` the predictor whenever the `update_status` column value from the `mindsdb.predictors` table is set to `available`.

Here is when the `update_status` column value is set to `available`:

- When the new version of MindsDB is available that causes the predictor to become obsolete.
- When the new data is available in the table that was used to train the predictor.

To find out whether you need to retrain your model, query the `mindsdb.predictors` table and look for the `update_status` column.

Here are the possible values of the `update_status` column:

| Name          | Description                                                                          |
| ------------- | ------------------------------------------------------------------------------------ |
| `available`   | It indicates that the model should be updated.                                       |
| `updating`    | It indicates that the retraining process of the model takes place.                   |
| `up_to_date`  | It indicates that your model is up to date and does not need to be retrained.        |

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

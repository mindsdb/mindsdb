# MindsDB Tables Structure

## MindsDB Default Tables

On start-up, the MindsDB database consists of 2 tables: `predictors` and `databases`.

You can verify it by running the following SQL command:

```sql
SHOW tables;
```

On execution, we get:

```
+----------------------+
| Tables_in_mindsdb    |
+----------------------+
| predictors           |
| databases            |
+----------------------+
```

## The `predictors` Table

At first, the `predictors` table is empty. But, as soon as you create your first machine learning model, you'll see it as a new record in the `predictors` table.

```sql
SELECT *
FROM mindsdb.predictors;
```

On execution, we get:

```
+-------+---------+-----------+----------+----------------+------------------+--------+--------------------+-------------------+
|name   |status   |accuracy   |predict   |update_status   |mindsdb_version   |error   |select_data_query   |training_options   |
+-------+---------+-----------+----------+----------------+------------------+--------+--------------------+-------------------+
|       |         |           |          |                |                  |        |                    |                   |
+-------+---------+-----------+----------+----------------+------------------+--------+--------------------+-------------------+
```

Where:

| Column name         | Description                                                                        |
| ------------------- | ---------------------------------------------------------------------------------- |
| `name`              | The name of the ML model.                                                          |
| `status`            | Training status having one of the values: generating, training, complete, error.   |
| `accuracy`          | The accuracy of the ML model.                                                      |
| `predict`           | The name of the target variable column to be predicted.                            |
| `update_status`     | Training update status (up_to_date, or updating).                                  |
| `mindsdb_version`   | The version of MindsDB under which this ML model was created.                      |
| `error`             | In the case of an error, it contains an error message.                             |
| `select_data_query` | SQL select query to create the datasource.                                         |
| `training options`  | Additional training parameters.                                                    |

### Example

To create a predictor, follow [this guide](https://docs.mindsdb.com/sql/create/predictor/).

```sql
SELECT *
FROM mindsdb.predictors;
```

On execution, we get:

```
+-----------------+--------+------------------+-------+-------------+---------------+-----+-----------------+----------------+
|name             |status  |accuracy          |predict|update_status|mindsdb_version|error|select_data_query|training_options|
+-----------------+--------+------------------+-------+-------------+---------------+-----+-----------------+----------------+
|house_sales_model|complete|0.4658770134240238|ma     |up_to_date   |22.7.5.1       |     |                 |                |
+-----------------+--------+------------------+-------+-------------+---------------+-----+-----------------+----------------+
```

## The `databases` Table

At first, the `databases` table contains two records:

* `files` is one of the default databases within the `mindsdb` database.
It stores all the files uploaded to the MindsDB Cloud editor. Following [this guide](https://docs.mindsdb.com/sql/create/file/), you can find out how to upload files to MindsDB.

* `views` is one of the default databases within the `mindsdb` database.
It stores all the views. [Here](https://docs.mindsdb.com/sql/create/view/) is how you can create a view in MindsDB.

```sql
SELECT *
FROM mindsdb.databases;
```

On execution, we get:

```
+-------+----------------+-------+-------+-------+
|name   |database_type   |host   |port   |user   |
+-------+----------------+-------+-------+-------+
|files  |files           |[NULL] |[NULL] |[NULL] |
|views  |views           |[NULL] |[NULL] |[NULL] |
+-------+----------------+-------+-------+-------+
```

Where:

| Column name       | Description                                      |
| ----------------- | ------------------------------------------------ |
| `name`            | The name of the database.                        |
| `database_type`   | The type or engine of the database.              |
| `host`            | The host of the database connected to MindsDB.   |
| `port`            | The port of the database connected to MindsDB.   |
| `user`            | The user of the database connected to MindsDB.   |

### Example

To create a database within MindsDB, follow [this guide](https://docs.mindsdb.com/sql/create/databases/).

```sql
SELECT *
FROM mindsdb.databases;
```

On execution, we get:

```
+-------------+----------------+---------------+-------+------------+
|name         |database_type   |host           |port   |user        |
+-------------+----------------+---------------+-------+------------+
|files        |files           |[NULL]         |[NULL] |[NULL]      |
|views        |views           |[NULL]         |[NULL] |[NULL]      |
|example_db   |postgres        |3.220.66.106   |5432   |demo_user   |
+-------------+----------------+---------------+-------+------------+
```

## The `[integration_name]` TABLE

!!! warning "This is a work in progress" 

## The model table

!!! warning "This is a work in progress" 

The below list contains the column names of the model table. Note that `target_variable_` will be the name of the target variable column.

| Column name                         | Description                                                                                                                   |
| ----------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `target_variable_original`          | The original value of the target variable.                                                                                    |
| `target_variable_min`               | Lower bound of the predicted value.                                                                                           |
| `target_variable_max`               | Upper bound of the predicted value.                                                                                           |
| `target_variable_confidence`        | Model confidence score.                                                                                                       |
| `target_variable_explain`           | JSON object that contains additional information as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth`.   |
| `select_data_query`                 | SQL select query to create the datasource.                                                                                    |

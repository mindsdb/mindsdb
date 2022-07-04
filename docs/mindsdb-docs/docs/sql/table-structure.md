# Table Structure

## General Structure

On startup the mindsdb database will contain 2 tables: `predictors` and `datasources`

```sql
SHOW TABLES;
```

On execution, you should get:

```sql

+---------------------------+
| Tables_in_mindsdb         |
+---------------------------+
| predictors                |
| databases                 |
| integration_name          |
+---------------------------+

```

## The predictors TABLE

All of the newly trained machine learning models will be visible as a new record inside the `predictors` table.
The `predictors` columns contains information about each model as:

| Column name         | Description                                  |
| ------------------- | -------------------------------------------- |
| `name`              | The name of the model.                       |
| `status`            | Training status(training, complete, error).  |
| `predict`           | The name of the target variable column.      |
| `accuracy`          | The model accuracy.                          |
| `update_status`     | Training update status(up_to_date, updating). |
| `mindsdb_version`   | The mindsdb version used.                    |
| `error`             | Error message info in case of an error.     |
| `select_data_query` | SQL select query to create the datasource.   |
| `training options`  | Additional training parameters.              |

## The datasource TABLE

!!! warning "This is a work in progress" 

## The `[integration_name]` TABLE

!!! warning "This is a work in progress" 

## The model table

!!! warning "This is a work in progress" 

The below list contains the column names of the model table. Note that `target_variable_` will be the name of the target variable column.

- target_variable_original - The original value of the target variable.
- target_variable_min - Lower bound of the predicted value.
- target_variable_max - Upper bound of the predicted value.
- target_variable_confidence - Model confidence score.
- target_variable_explain - JSON object that contains additional information as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth`.
- select_data_query - SQL select query to create the datasource.

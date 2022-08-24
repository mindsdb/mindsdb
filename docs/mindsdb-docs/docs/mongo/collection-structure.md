# Collection Structure

## General Structure

On start-up, the MindsDB database consists of 2 collections: `databases` and `predictors`.

You can verify it by running the following SQL commands:

```sql
USE mindsdb;
SHOW collections;
```

On execution, we get:

```sql
+---------------------------+
| Collections_in_mindsdb    |
+---------------------------+
| databases                 |
| predictors                |
+---------------------------+
```

## The `predictors` Collection

All the trained machine learning models are visible as new documents inside the `predictors` collection.

The `predictors` collection stores information about each model in the JSON format, as shown below.

```json
{ 
    "name" : "model_name",
    "status" : "status",
    "accuracy" : 0.999,
    "predict" : "value_to_be_predicted",
    "update_status" : "update_status",
    "mindsdb_version" : "22.8.2.1",
    "error" : "error_info",
    "select_data_query" : "",
    "training_options" : ""
}
```

Where:

| Name                       | Description                                                                           |
| -------------------------- | ------------------------------------------------------------------------------------- |
| "name"                     | The name of the model                                                                 |
| "status"                   | Training status (`generating`, or `training`, or `complete`, or `error`)              |
| "accuracy"                 | The model accuracy (`0.999` is a sample accuracy value)                               |
| "predict"                  | The name of the target variable column                                                |
| "update_status"            | Training update status (`up_to_date`, or `updating`)                                  |
| "mindsdb_version"          | The MindsDB version used while training (`22.8.2.1` is a sample version value)        |
| "error"                    | Error message stores a value in case of an error, otherwise, it is null               |
| "select_data_query"        | It is required for SQL API, otherwise, it is null                                     |
| "training_options"         | Additional training parameters                                                        |

## The `databases` Collection

All the Mongo database connections are stored inside the `databases` collection, as shown below.

```json
{ 
"name" : "mongo_int",
"database_type" : "mongodb",
"host" : "",
"port" : 27017,
"user" : null
}
```

Where:

| Name                | Description                                 |
| ------------------- | ------------------------------------------- |
| "name"              | The name of the integration                 |
| "database_type"     | The database type (it is always `mongodb`)  |
| "host"              | The Mongo host                              |
| "port"              | The Mongo port                              |
| "user"              | The Mongo user                              |

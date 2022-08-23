# Collection Structure

## General Structure

On startup the mindsdb database will contain 2 collections: `databases` and `predictors`. 

```mql
use mindsdb;
show collections;
```

On execution, you should get:

```sql

+---------------------------+
| Collections_in_mindsdb    |
+---------------------------+
| databases                 |
| predictors                |
+---------------------------+

```

## The predictors Collection

All of the newly trained machine learning models will be visible as a new document inside the `predictors` collection.
The `predictors` collection should have information about each model as:

```json
{ 
    "name" : "", # The name of the model.
    "status" : "", # Training status(training, complete, error).
    "accuracy" : , # The name of the target variable column.
    "predict" : "", # The model accuracy. 
    "update_status" : "", # Training update status(up_to_date, updating)
    "mindsdb_version" : "", # The mindsdb version used.
    "error" : "", # Error message info in case of an error.
    "select_data_query" : "", # empty, required for SQL API.
    "training_options" : "" # Additional training parameters. 
}
```

## The databases Collection

All of the Mongo connections will be stored inside the database collection as:


```json
{ 
"name" : "mongo_int", # The name of integration
"database_type" : "mongodb",  # The database type. Always mongo
"host" : "", # The connection string
"port" : 27017,  # Mongo port
"user" : null  # Mongo user
}
```
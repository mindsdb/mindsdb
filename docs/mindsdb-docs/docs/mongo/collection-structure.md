# Collection Structure

## General Structure

On startup the mindsdb database will contain 2 collections: `predictors` and `databases`

```mql
use mindsdb;
show collections;
```

On execution, you should get:

```sql

+---------------------------+
| Collections_in_mindsdb    |
+---------------------------+
| predictors                |
| databases                 |
+---------------------------+

```

## The predictors Collection

All of the newly trained machine learning models will be visible as a new document inside the `predictors` collection.
The `predictors` columns contains information about each model as:

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

## The datasource Collection

!!! warning "This is a work in progress" 

## The `[integration_name]` Collection

!!! warning "This is a work in progress" 

## The model Collection

!!! warning "This is a work in progress" 

The below list contains the column names of the model collection. Note that `target_variable_` will be the name of the target variable column.

- target_variable_original - The original value of the target variable.
- target_variable_min - Lower bound of the predicted value.
- target_variable_max - Upper bound of the predicted value.
- target_variable_confidence - Model confidence score.
- target_variable_explain - JSON object that contains additional information as `confidence_lower_bound`, `confidence_upper_bound`, `anomaly`, `truth`.
- select_data_query - empty, required for SQL API
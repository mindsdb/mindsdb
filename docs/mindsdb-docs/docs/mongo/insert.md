# `#!sql predictors.insert()` Method

## Description

The `db.predictors.insert()` method is used to train a new model. The basic syntax for training a model is:

## Syntax

```
db.predictors.insert(
{
     "name": "<predictor_name>",
     "predict": "<target_column>",
     "connection": "<integration_name>",
     "select_data_query": {
        'collection': '<collection_name',
        'call': [
            'method': 'find',
            'args': []
        ] 
     } 
})
```

On execution, you should get:

```json
WriteResult({ "nInserted" : 1 })
```

Where:

| Expressions                                     | Description                                                                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `<predictor_name>`                              | Name of the model to be created                                                                                                       |
| `<target_column>`                       | where `target_column` is the column name of the target variable. 
| `<integration_name>`                            | is the name of the integration created via [`#!sql INSERT DATABASE`](/mongo/database/) or [file upload](/sql/api/select_files/) |
| `select_data_query` | Object that has the the data collection name to be used for training and validation  and additional arguments for filtering the data.                                                 |
                                                                     |

!!! TIP "Checking the status of the model"
After you run the `#!sql INSERT PREDICTOR` method, you can check the status of the training model, by calling the find function from the `#!sql mindsdb.predictors` collection:

    ```
    db.predictors.find({'name': "<model_name>"})
    ```

## Example

This example shows how you can train a Machine Learning model called home_rentals_model to predict the rental prices for real estate properties inside the dataset.

```
db.predictors.insert(
{
     "name": "home_rentals_model",
     "predict": "rental_price",
     "connection": "mongo_integration",
     "select_data_query": {
        'collection': 'home_rentals',
        'call': [{
            'method': 'find',
            'args': []
        }] 
     } 
})
```

On execution:

```json
WriteResult({ "nInserted" : 1 })
```

To check the predictor status query the [`#!sql mindsdb.predictors`](/mongo/collection-structure/#the-predictors-collection):

```
db.predictors.find({'name': "home_rentals_model"})
```

On execution, we get:
 
```json
{ 
    "name" : "home_rentals_model", 
    "status" : "complete", 
    "accuracy" : "0.91", 
    "predict" : "rental_price", 
    "update_status" : "up_to_date", 
    "mindsdb_version" : "22.8.3.1", 
    "error" : null,
    "select_data_query" : "", 
    "training_options" : ""
}
```


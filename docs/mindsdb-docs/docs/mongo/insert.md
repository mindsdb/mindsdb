# Creating Predictors in Mongo

Predictors are the machine learning models that enable us to forecast future data based on the available data. By using the `db.predictors.insert()` method, we create and train predictors in Mongo.

## Using the `#!sql db.predictors.insert()` Method

### Description

The `db.predictors.insert()` method creates and trains a new model.

### Syntax

```sql
db.predictors.insert(
{
     "name": "<predictor_name>",
     "predict": "<target_column>",
     "connection": "<integration_name>",
     "select_data_query": {
        'collection': '<collection_name>',
        'call': [
            'method': 'find',
            'args': []
        ] 
     } 
});
```

On execution, we get:

```json
WriteResult({ "nInserted" : 1 })
```

Where:

| Expressions                                     | Description                                                                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `<predictor_name>`                              | The name of the model to be created                                                                                                   |
| `<target_column>`                               | The column name of the target variable                                                                                                |
| `<integration_name>`                            | The name of the integration created via [`#!sql INSERT DATABASE`](/mongo/database/) or [file upload](/sql/api/select_files/)          |
| `select_data_query`                             | Object that stores the data collection name to be used for training and validation and additional arguments for filtering the data.   |

!!! TIP "Checking the status of the model"
    After running the `db.predictors.insert()` method, execute the `find` function from the `mindsdb.predictors` collection to check the status of the model.

    ```sql
    db.predictors.find({'name': "<model_name>"});
    ```

## Example

### Creating a Predictor

This example shows how you can train the `home_rentals_model` machine learning model to predict the rental prices for real estate properties inside the dataset.

```sql
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
});
```

On execution, we get:

```json
WriteResult({ "nInserted" : 1 })
```

### Checking Predictor Status

To check the predictor status, query the [`mindsdb.predictors`](/mongo/collection-structure/#the-predictors-collection) using the `db.predictors.find({})` command.

```sql
db.predictors.find({'name': "home_rentals_model"});
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

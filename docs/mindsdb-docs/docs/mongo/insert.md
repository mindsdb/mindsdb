# Creating Predictors in Mongo

Predictors are the machine learning models that enable us to forecast future data based on the available data. By using the `db.predictors.insert()` method, we create and train predictors in Mongo.

## The `db.predictors.insert()` Method

### Description

The `db.predictors.insert()` method creates and trains a new model.

### Syntax

Here is the syntax:

```sql
db.predictors.insert({
     name: "predictor_name",
     predict: "target_column",
     connection: "integration_name",
     select_data_query: {
        "collection": "collection_name",
        "call": [
            "method": "find",
            "args": []
        ]
     }
});
```

On execution, we get:

```json
WriteResult({
    "nInserted" : 1
})
```

Where:

| Expressions                                     | Description                                                                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `name`                                          | The name of the model to be created.                                                                                                  |
| `predict`                                       | The name of the target column to be predicted.                                                                                        |
| `connection`                                    | The name of the integration created via [the `db.databases.insertOne()` method](/mongo/database/) or [file upload](/sql/create/file/).|
| `select_data_query`                             | Object that stores the data collection name to be used for training and validation and additional arguments for filtering the data.   |

!!! TIP "Checking Predictor Status"
    After running the `db.predictors.insert()` method, execute the `db.predictors.find()` method from the `mindsdb.predictors` collection to check the status of the model.

    ```sql
    db.predictors.find({name: "model_name"});
    ```

## Example

### Creating a Predictor

This example shows how you can create and train the `home_rentals_model` machine learning model to predict the rental prices for real estate properties inside the dataset.

```sql
db.predictors.insert({
     name: "home_rentals_model",
     predict: "rental_price",
     connection: "mongo_integration",
     select_data_query: {
        "collection": "home_rentals",
        "call": [{
            "method": "find",
            "args": []
        }]
     }
});
```

On execution, we get:

```json
WriteResult({
    "nInserted" : 1
})
```

### Checking Predictor Status

To check the predictor status, query the [`mindsdb.predictors`](/mongo/collection-structure/#the-predictors-collection) using the `db.predictors.find()` command.

```sql
db.predictors.find({name: "home_rentals_model"});
```

On execution, we get:
 
```json
{ 
    "name" : "home_rentals_model", 
    "status" : "complete", 
    "accuracy" : 0.91, 
    "predict" : "rental_price", 
    "update_status" : "up_to_date", 
    "mindsdb_version" : "22.8.3.1", 
    "error" : null,
    "select_data_query" : "", 
    "training_options" : ""
}
```

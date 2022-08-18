# `#!sql predictors.deleteOne()` Method

## Description

The `#!sql predictors.deleteOne()` method is called to delete the model collection:

## Syntax

```sql
db.predictors.deleteOne({'name': <predictor_name>});
```

On execution, we get:

```json
{ "acknowledged" : true, "deletedCount" : 1 }
```

Where:

| Name               | Description                     |
| ------------------ | ------------------------------- |
| `<predictor_name>` | Name of the model to be deleted |


## Example

The following MQL statement drops the model collection called `home_rentals_model`. Given the following mongo query to list all predictors by name

```sql
db.predictors.find({})
```

On execution, we get:

```JSON
{
    "name": "home_rentals_model",
    "status": "complete",
    "accuracy": "1.0",
    "predict": "rental_price",
    "update_status": "up_to_date",
    "mindsdb_version": "22.8.3.1",
    "error": null,
    "select_data_query": "",
    "training_options": ""
},
{
    "name": "other_model",
    "status": "complete",
    "accuracy": "1.0",
    "predict": "rental_price",
    "update_status": "up_to_date",
    "mindsdb_version": "22.8.3.1",
    "error": null,
    "select_data_query": "",
    "training_options": ""
}
```

Execute the `#!sql deleteOne()` method as:

```
db.predictors.deleteOne({'name': 'home_rentals_model'})
```

On execution, we get:

```json
{ "acknowledged" : true, "deletedCount" : 1 }
```

Validate that the model has been deleted by listing again all predictors by name:

```
db.predictors.find({})
```

On execution, we get:

```json
{
    "name": "other_model",
    "status": "complete",
    "accuracy": "1.0",
    "predict": "rental_price",
    "update_status": "up_to_date",
    "mindsdb_version": "22.8.3.1",
    "error": null,
    "select_data_query": "",
    "training_options": ""
}
```

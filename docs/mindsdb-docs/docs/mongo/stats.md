# Getting the Statistics

## The `stats()` Method

The `stats()` method is used to display the attributes of an existing model. It accepts the `{'scale': 'attribute'}` object as an argument.

Here is how to call the `stats()` method.

```sql
db.name_of_your_predictor.stats({'scale':'attribute'});
```

Where:

| Name                       | Description                                                                                                                                       |
| ---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `name_of_your_predictor`   | The name of the predictor whose statistics you want to see                                                                                        |
| `{'scale': 'attribute'}`   | The argument of the `stats()` method defines the type of statistics (`{'scale':'features'}`, or `{'scale':'model'}`, or `{'scale':'ensemble'}`)   |

## Using the `stats()` Method with the `{'scale':'features'}` Parameter

### Description

The `db.name_of_your_predictor.stats({'scale':'features'})` method is used to display the way the model encoded the data before training.

### Syntax

```sql
db.name_of_your_predictor.stats({'scale':'features'});
```

On execution, we get:

```json
{
  "data":[
    {
       "column" : "number_of_rooms",
       "type" : "categorical",
       "encoder" : "OneHotEncoder",
       "role" : "feature"
    }
  ]
}
```

Where:

| Name            | Description                                      |
| --------------- | ------------------------------------------------ |
| `column`        | The name of the column                           |
| `type`          | Type of the inferred data                        |
| `encoder`       | Encoder used                                     |
| `role`          | Role of the column (`feature` or `target`)       |

### Example

```sql
db.home_rentals_model.stats({'scale':'features'});
```

On execution, we get:

```json
{
        "data" : [
                {
                        "column" : "number_of_rooms",
                        "type" : "categorical",
                        "encoder" : "OneHotEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "number_of_bathrooms",
                        "type" : "binary",
                        "encoder" : "BinaryEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "sqft",
                        "type" : "float",
                        "encoder" : "NumericEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "location",
                        "type" : "categorical",
                        "encoder" : "OneHotEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "days_on_market",
                        "type" : "integer",
                        "encoder" : "NumericEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "initial_price",
                        "type" : "integer",
                        "encoder" : "NumericEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "neighborhood",
                        "type" : "categorical",
                        "encoder" : "OneHotEncoder",
                        "role" : "feature"
                },
                {
                        "column" : "rental_price",
                        "type" : "float",
                        "encoder" : "NumericEncoder",
                        "role" : "target"
                }
        ],
        "ns" : "mindsdb.home_rentals_model"
}
```

## Using the `stats()` Method with the `{'scale':'model'}` Parameter

### Description

The `db.name_of_your_predictor.stats({'scale':'model'})` method is used to display the performance of the candidate models.

### Syntax

```sql
db.name_of_your_predictor.stats({'scale':'model'});
```

On execution, we get:

```json
{
  "data" :[
      {
              "name" : "<candidate_model>",
              "performance" : <0.0|1.0>,
              "training_time" : <seconds>,
              "selected" : <0|1>
      },
  ]
}
```

Where:

| Name                       | Description                                                |
| -------------------------- | ---------------------------------------------------------- |
| `name`                     | Name of the candidate model                                |
| `performance`              | Accuracy from 0 to 1 depending on the type of the model    |
| `training_time`            | Time elapsed for the training of the model                 |
| `selected`                 | `1` for the best performing model and `0` for the rest     |

### Example

```sql
db.home_rentals_model.stats({'scale':'model'});
```

On execution, we get:

```json
{
        "data" : [
                {
                        "name" : "Neural",
                        "performance" : 0.999,
                        "training_time" : 48.37,
                        "selected" : 0
                },
                {
                        "name" : "LightGBM",
                        "performance" : 1,
                        "training_time" : 33,
                        "selected" : 1
                },
                {
                        "name" : "Regression",
                        "performance" : 0.999,
                        "training_time" : 0.05,
                        "selected" : 0
                }
        ],
        "ns" : "mindsdb.home_rentals_model"
}
```

## Using the `stats()` Method with the `{'scale':'ensemble'}` Parameter

### Description

The `db.name_of_your_predictor.stats({'scale':'ensemble'})` method is used to display the parameters used to select the best candidate model.

### Syntax

```sql
db.name_of_your_predictor.stats({'scale':'ensemble'});
```

On execution, we get:

```sql
+-----------------+
| ensemble        |
+-----------------+
| {JSON}          |
+-----------------+
```

Where:

| Name       | Description                                                                               |
| ---------- | ----------------------------------------------------------------------------------------- |
| `ensemble` | Object of the JSON type describing the parameters used to select the best candidate model |

### Example

!!! warning "Example WIP"
    This example is a work in progress.

!!! TIP "Unsure what it all means?"
    If you're unsure how to `#!sql DESCRIBE` your model or understand the results, feel free to ask us at the community [Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

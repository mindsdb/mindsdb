# `#!sql stats()` Method

## Description

The `stats()` method is used to display the attributes of an existing model. It accept the {'scale': attribute} object.

## `#!sql {'scale':'features'}` Parameter

### `#!sql stats({'scale':'features'})` Description

The `db.[name_of_your_predictor].stats({'scale':'features'})` method is used to display the way that the model encoded the data prior to training.

### `#!sql stats({'scale':'features'})` Syntax

```sql
db.<name_of_your_predictor>.stats({'scale':'features'});
```

On execution:

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

|                            | Description                                           |
| -------------------------- | ----------------------------------------------------- |
| `column` | The name of the column                      |
| `type`                     | Type of data inferred                                        |
| `encoder`                    | Encoder used                                          |
| role                       | Role for that column, it can be `feature` or `target` |

### `#!sql stats({'scale':'features'})` Example

```sql
db.home_rentals_model.stats({'scale':'features'})
```

On execution:

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

## `#!sql {'scale':'model'}` Parameter

The `db.<name_of_your_predictor>.stats({'scale':'model'})` method is used to display the performance of the candidate models.

### `#!sql stats({'scale':'model'})` Syntax

```sql
db.<name_of_your_predictor>.stats({'scale':'model'})
```

On execution:

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

|                            | Description                                            |
| -------------------------- | ------------------------------------------------------ |
| `[name_of_your_predictor]` | Name of the model to be described                      |
| name                       | Name of the candidate_model                            |
| performance                | Accuracy From 0 - 1 depending on the type of the model |
| training_time              | Time elapsed for the model training to be completed    |
| selected                   | `1` for the best performing model `0` for the rest     |

### `#!sql stats({'scale':'model'})` Example

```sql
db.home_rentals_model.stats({'scale':'model'})
```

On execution:

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

## `#!sql {'scale':'ensemble'}` Parameter


The `db.<name_of_your_predictor>.stats({'scale':'ensemble'})` method is used to display the parameters used to select best model candidate.

### `#!sql stats({'scale':'ensemble'})` Syntax

```sql
db.<name_of_your_predictor>.stats({'scale':'ensemble'});
```

On execution:

```sql
+-----------------+
| ensemble        |
+-----------------+
| {JSON}          |
+-----------------+
```

Where:

|          | Description                                                                    |
| -------- | ------------------------------------------------------------------------------ |
| ensemble | JSON type object describing the parameters used to select best model candidate |

### `#!sql DESCRIBE ... ENSEMBLE` Example WIP


!!! TIP "Unsure what it all means?"
    If you're unsure on how to `#!sql DESCRIBE` your model or understand the results feel free to ask us how to do it on the community [Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

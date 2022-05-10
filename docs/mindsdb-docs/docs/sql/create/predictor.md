# `#!sql CREATE PREDICTOR` Statement

## Description

The `CREATE PREDICTOR` statement is used to train a new model. The basic syntax for training a model is:

## Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
    (SELECT [column_name, ...] FROM [table_name])
PREDICT [target_column]
```

Where:

| Expressions                                     | Description                                                                   |
| ----------------------------------------------- | ----------------------------------------------------------------------------- |
| `[predictor_name]`                              | Name of the model to be created                                               |
| `[integration_name]`                            | is the name of the [datasource](/connect/#create-new-datasource)              |
| `(SELECT [column_name, ...] FROM [table_name])` | SELECT statement for selecting the data to be used for traning and validation |
| `PREDICT [target_column]`                         | where `target_column` is the column name of the target variable.                |

## Example

## `#!sql USING` Statement

### Description

In MindsDB, the underlying AutoML models are based on Lightwood. This library generates models automatically based on the data and a declarative problem definition, but the default configuration can be overridden. The `#!sql USING ...` statement provides the option to configure a model to be trained with specific options.

### `#!sql USING` Statement Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
    (SELECT [column_name, ...] FROM [table_name])
PREDICT [target_column]
USING [parameter_key] = ['parameter_value']
```

| parameter key                               | Description                                                                                                                                                                                                                                               |
| ------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `encoders`                                  | Grants access to configure how each column is encoded.By default, the AutoML engine will try to get the best match for the data. To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).                   |
| `model`                                     | Allows you to specify what type of Machine Learning algorithm to learn from the encoder data. To learn more about all the model options, go [here](https://lightwood.io/mixer.html).                                                                      |
| Other keys supported by lightwood in JsonAI | he most common usecases for configuring predictors will be listed and explained in the example below. To see all options available in detail, you should checkout the [lightwood docs about JsonAI](https://lightwood.io/api/types.html#api.types.JsonAI) |

### `#!sql ... USING encoders` Key

Grants access to configure how each column is encoded. To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).

```sql
...
USING
encoders.[column_name].module='value';
```

!!! tip "By default, the AutoML engine will try to get the best match for the data."

### `#!sql ... USING model` Key

Allows you to specify what type of Machine Learning algorithm to learn from the encoder data. To learn more about all the model options, go [here](https://lightwood.io/mixer.html)

```sql
...
USING
model.args='{"key": value}'
;
```

### `#!sql USING` Example

We will use the home rentals dataset, specifying particular encoders for some of the columns and a LightGBM model.

```sql
CREATE PREDICTOR mindsdb.home_rentals_predictor
FROM my_db_integration (
    SELECT * FROM home_rentals
) PREDICT rental_price
USING
    encoders.location.module='CategoricalAutoEncoder',
    encoders.rental_price.module = 'NumericEncoder',
    encoders.rental_price.args.positive_domain = 'True',
    model.args='{"submodels":[
                    {"module": "LightGBM",
                     "args": {
                         "stop_after": 12,
                          "fit_on_dev": true
                          }
                    }    
                ]}';
```

## `#!sql CREATE PREDICTOR` For Time Series Models

### Description

To train a timeseries model, MindsDB provides additional statements.

### Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
(SELECT [sequential_column], [partition_column], [other_column], [target_column] FROM [table_name])
PREDICT [target_column]

ORDER BY [sequantial_column]
GROUP BY [partition_column]

WINDOW [int]
HORIZON [int];
```

Where:

| Expressions                                     | Description                                                                   |
| ----------------------------------------------- | ----------------------------------------------------------------------------- |
| `ORDER BY [sequantial_column]`                  | Defines the column that the time series will be order by. These can be a date, or anything that defines the sequence of events.|
| `GROUP BY [partition_column]` (optional)        |  Groups the rows that make a partition, for example, if you want to forecast inventory for all items in a store, you can partition the data by product_id, meaning that each product_id has its own time series. |
| `WINDOW [int]` | Specifies the number `[int]` of rows to "look back" into when making a prediction after the rows are ordered by the order_by column and split into groups. This could be interpreted like "Always use the previous 10 rows". |
| `HORIZON [int]` (optional)                      |  keyword specifies the number of future predictions, default value is 1  |

### Example

```sql
CREATE PREDICTOR mindsdb.inventory_model
FROM db_integration
(SELECT * FROM inventory) as inventory
PREDICT units_in_inventory as predicted_units_in_inventory

ORDER BY date,
GROUP BY product_id,

WINDOW 20
HORIZON 7

```

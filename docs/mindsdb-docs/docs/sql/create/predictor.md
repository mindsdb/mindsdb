# `#!sql CREATE PREDICTOR` Statement

## Description

The `CREATE PREDICTOR` statement is used to train a new model. The basic syntax for training a model is as follows:

## Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
    (SELECT [column_name, ...] FROM [table_name])
PREDICT [target_column];
```

Where:

| Expressions                                     | Description                                                                                                                           |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| `[predictor_name]`                              | Name of the model to be created                                                                                                       |
| `[integration_name]`                            | is the name of the integration created via [`#!sql CREATE DATABASE`](/sql/create/databases/) or [file upload](/sql/api/select_files/) |
| `(SELECT [column_name, ...] FROM [table_name])` | SELECT statement for selecting the data to be used for training and validation                                                        |
| `PREDICT [target_column]`                       | where `target_column` is the column name of the target variable.                                                                      |

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

!!! TIP "Checking the status of the model"
After you run the `#!sql CREATE PREDICTOR` statement, you can check the status of the training model by selecting from the `#!sql mindsdb.predictors`.

```sql
SELECT *
FROM mindsdb.predictors
WHERE name='[predictor_name]';
```

On execution, we get:

```sql
+------------------------+---------------------+-------------------------+-----------------------+-------------+---------------+-----+-----------------+----------------+
|name                    |status               |accuracy                 |predict                |update_status|mindsdb_version|error|select_data_query|training_options|
+------------------------+---------------------+-------------------------+-----------------------+-------------+---------------+-----+-----------------+----------------+
|predictor_name          |complete or training |[number between 0 and 1] |column_to_be_predicted |up_to_date   |22.7.5.0       |     |                 |                |
+------------------------+---------------------+-------------------------+-----------------------+-------------+---------------+-----+-----------------+----------------+
```

## Example

This example shows how you can train a Machine Learning model called home_rentals_model to predict the rental prices for real estate properties inside the dataset.

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM db_integration (SELECT * FROM house_rentals_data) as rentals
PREDICT rental_price as price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

To check the predictor status, query the [`#!sql mindsdb.predictors`](/sql/table-structure/#the-predictors-table) table.

```sql
SELECT * FROM mindsdb.predictors WHERE name='home_rentals_model';
```

On execution, we get:

```sql
+--------------------+----------+--------------------+--------------+---------------+-----------------+-------+-------------------+------------------+
| name               | status   | accuracy           | predict      | update_status | mindsdb_version | error | select_data_query | training_options |
+--------------------+----------+--------------------+--------------+---------------+-----------------+-------+-------------------+------------------+
| home_rentals_model | complete | 0.9991920992432087 | rental_price | up_to_date    | 22.5.1.0        | NULL  |                   |                  |
+--------------------+----------+--------------------+--------------+---------------+-----------------+-------+-------------------+------------------+
```

## `#!sql ... USING` Statement

### `#!sql ... USING` Description

In MindsDB, the underlying AutoML models are based on [Lightwood](https://lightwood.io/). This library generates models automatically based on the data and a declarative problem definition, but the default configuration can be overridden. The `#!sql USING ...` statement provides the option to configure a model to be trained with specific options.

### `#!sql ... USING` Statement Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
    (SELECT [column_name, ...] FROM [table_name])
PREDICT [target_column]
USING [parameter_key] = ['parameter_value'];
```

Where:

| parameter key                               | Description                                                                                                                                                                                                                                                |
| ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `encoders`                                  | Grants access to configure how each column is encoded.By default, the AutoML engine will try to get the best match for the data. To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).                    |
| `model`                                     | Allows you to specify what type of Machine Learning algorithm to learn from the encoder data. To learn more about all the model options, go [here](https://lightwood.io/mixer.html).                                                                       |
| Other keys supported by lightwood in JsonAI | The most common usecases for configuring predictors will be listed and explained in the example below. To see all options available in detail, you should checkout the [lightwood docs about JsonAI](https://lightwood.io/api/types.html#api.types.JsonAI) |

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

### `#!sql ... USING encoders` Key

It grants access to configure how each column is encoded. To learn more about how encoders work and their options, go [here](https://lightwood.io/encoder.html).

```sql
...
USING
encoders.[column_name].module='value';
```

!!! tip "By default, the AutoML engine will try to get the best match for the data."

### `#!sql ... USING model` Key

It allows you to specify what type of Machine Learning algorithm to learn from the encoder data.

```sql
...
USING
model.args='{"key": value}';
```

Module options:

| Module                                                                   | Description                                                                                                                                                                                                                                                               |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [**BaseMixer**](https://lightwood.io/mixer.html#mixer.BaseMixer)         | Base class for all mixers.                                                                                                                                                                                                                                                |
| [**LightGBM**](https://lightwood.io/mixer.html#mixer.LightGBM)           | This mixer configures and uses LightGBM for regression or classification tasks depending on the problem definition.                                                                                                                                                       |
| [**LightGBMArray**](https://lightwood.io/mixer.html#mixer.LightGBMArray) | This mixer is composed of several LightGBM mixers in regression mode aimed at time series forecasting tasks.                                                                                                                                                              |
| [**NHitsMixer**](https://lightwood.io/mixer.html#mixer.NHitsMixer)       | Wrapper around a MQN-HITS deep learning model.                                                                                                                                                                                                                            |
| [**Neural**](https://lightwood.io/mixer.html#mixer.Neural)               | The Neural mixer trains a fully connected dense network from concatenated encoded outputs of each of the features in the dataset to predicted the encoded output.                                                                                                         |
| [**ProphetMixer**](https://lightwood.io/mixer.html#mixer.ProphetMixer)   | This mixer is a wrapper around the popular time series library sktime.                                                                                                                                                                                                    |
| [**Regression**](https://lightwood.io/mixer.html#mixer.Regression)       | The Regression mixer inherits from [scikit-learn’s Ridge class](https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.Ridge.html)                                                                                                                        |
| [**SkTime**](https://lightwood.io/mixer.html#mixer.SkTime)               | This mixer is a wrapper around the popular time series library sktime.                                                                                                                                                                                                    |
| [**Unit**](https://lightwood.io/mixer.html#mixer.Unit)                   | Special mixer that passes along whatever prediction is made by the target encoder without modifications. This is useful for single-column predictive scenarios that may involve complex and/or expensive encoders (e.g. free-form text classification with transformers). |

To learn more about all the model options, please refer to [Lightwood's documentation here.](https://lightwood.io/mixer.html)

### `#!sql ... USING` Example

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

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

## `#!sql CREATE PREDICTOR` From file

In order to create a predictor from a file, you should first upload a file to MindsDB. Follow [this guide](https://docs.mindsdb.com/sql/create/file/) to see how to do that.

### `#!sql CREATE PREDICTOR` Description

This statement is used to create and train a model using a file or a database table.

### `#!sql CREATE PREDICTOR` Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM files
    (SELECT * FROM [file_name])
PREDICT target_variable;
```

Where:

|                               | Description                                                                    |
| ----------------------------- | ------------------------------------------------------------------------------ |
| `[predictor_name]`            | Name of the model to be created                                                |
| `[file_name]`                 | Name of the file uploaded via the MindsDB editor                               |
| `(SELECT * FROM [file_name])` | SELECT statement for selecting the data to be used for training and validation |
| `target_variable`             | `target_column` is the column name of the target variable.                     |

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

### `#!sql CREATE PREDICTOR` Example

```sql
CREATE PREDICTOR mindsdb.home_rentals_model
FROM files
    (SELECT * from home_rentals)
PREDICT rental_price;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

## `#!sql CREATE PREDICTOR` For Time Series Models

### `#!sql CREATE PREDICTOR` For Time Series Models Description

To train a timeseries model, MindsDB provides additional statements.

### `#!sql CREATE PREDICTOR` For Time Series Models Syntax

```sql
CREATE PREDICTOR mindsdb.[predictor_name]
FROM [integration_name]
(SELECT [sequential_column], [partition_column], [other_column], [target_column] FROM [table_name])
PREDICT [target_column]

ORDER BY [sequential_column]
GROUP BY [partition_column]

WINDOW [int]
HORIZON [int];
```

Where:

| Expressions                              | Description                                                                                                                                                                                                                  |
| ---------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ORDER BY [sequential_column]`           | Defines the column that the time series will be order by. These can be a date, or anything that defines the sequence of events.                                                                                              |
| `GROUP BY [partition_column]` (optional) | Groups the rows that make a partition, for example, if you want to forecast inventory for all items in a store, you can partition the data by product_id, meaning that each product_id has its own time series.              |
| `WINDOW [int]`                           | Specifies the number `[int]` of rows to "look back" into when making a prediction after the rows are ordered by the order_by column and split into groups. This could be interpreted like "Always use the previous 10 rows". |
| `HORIZON [int]` (optional)               | keyword specifies the number of future predictions, default value is 1                                                                                                                                                       |

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

!!! warning "Getting a prediction of a Time Series model"
Due to the nature of Time Series Forecasting you will need to use the [`#!sql JOIN`](/sql/api/join) statement to get results.

### `#!sql CREATE PREDICTOR` For Time Series Models Example

```sql
CREATE PREDICTOR mindsdb.inventory_model
FROM db_integration
(SELECT * FROM inventory) as inventory
PREDICT units_in_inventory as predicted_units_in_inventory

ORDER BY date,
GROUP BY product_id,

WINDOW 20
HORIZON 7;
```

On execution, we get:

```sql
Query OK, 0 rows affected (x.xxx sec)
```

# `#!sql DESCRIBE` Statement

The `DESCRIBE` statement is used to display the attributes of an existing model.

## `#!sql DESCRIBE ... FEATURES` Statement

### Description

The `DESCRIBE mindsdb.[predictor_name].features` statement displays how the model encoded the data before the training process.

### Syntax

Here is the syntax:

```sql
DESCRIBE mindsdb.[predictor_name].features;
```

On execution, we get:

```sql
+--------------+-------------+--------------+-------------+
| column       | type        | encoder      | role        |
+--------------+-------------+--------------+-------------+
| column_name  | column_type | encoder_used | column_role |
+--------------+-------------+--------------+-------------+
```

Where:

| Name                 | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `[predictor_name]`   | Name of the model to be described.                    |
| `column`             | Data columns that were used to create the model.      |
| `type`               | Data type of the column.                              |
| `encoder`            | Encoder type used for the column.                     |
| `role`               | Role of the column (`feature` or `target`).           |

### Example

Let's look at an example using the `home_rentals_model` model.

```sql
DESCRIBE mindsdb.home_rentals_model.features;
```

On execution, we get:

```sql
+---------------------+-------------+----------------+---------+
| column              | type        | encoder        | role    |
+---------------------+-------------+----------------+---------+
| number_of_rooms     | categorical | OneHotEncoder  | feature |
| number_of_bathrooms | binary      | BinaryEncoder  | feature |
| sqft                | integer     | NumericEncoder | feature |
| location            | categorical | OneHotEncoder  | feature |
| days_on_market      | integer     | NumericEncoder | feature |
| neighborhood        | categorical | OneHotEncoder  | feature |
| rental_price        | integer     | NumericEncoder | target  |
+---------------------+-------------+----------------+---------+
```

Here the `rental_price` column is the `target` column to be predicted. As for the `feature` columns, these are used to train the ML model to predict the value of the `rental_price` column.

## `#!sql DESCRIBE ... MODEL` Statement

### Description

The `DESCRIBE mindsdb.[predictor_name].model` statement displays the performance of the candidate models.

### Syntax

Here is the syntax:

```sql
DESCRIBE mindsdb.[predictor_name].model;
```

On execution, we get:

```sql
+-----------------+-------------+---------------+-----------+---------------------+
| name            | performance | training_time | selected  | accuracy_functions  |
+-----------------+-------------+---------------+-----------+---------------------+
| candidate_model | performance | training_time | selected  | accuracy_functions  |
+-----------------+-------------+---------------+-----------+---------------------+
```

Where:

| Name                       | Description                                                     |
| -------------------------- | --------------------------------------------------------------- |
| `[predictor_name]`         | Name of the model to be described.                              |
| `name`                     | Name of the candidate model.                                    |
| `performance`              | Accuracy value from 0 to 1, depending on the type of the model. |
| `training_time`            | Time elapsed for the training of the model.                     |
| `selected`                 | `1` for the best performing model and `0` for the rest.         |
| `accuracy_functions`       |                                                                 |

### Example

Let's look at an example using the `home_rentals_model` model.

```sql
DESCRIBE mindsdb.home_rentals_model.model;
```

On execution, we get:

```sql
+------------+--------------------+----------------------+----------+---------------------+
| name       | performance        | training_time        | selected | accuracy_functions  |
+------------+--------------------+----------------------+----------+---------------------+
| Neural     | 0.9861694189913056 | 3.1538941860198975   | 0        | ['r2_score']        |
| LightGBM   | 0.9991920992432087 | 15.671080827713013   | 1        | ['r2_score']        |
| Regression | 0.9983390488042778 | 0.016761064529418945 | 0        | ['r2_score']        |
+------------+--------------------+----------------------+----------+---------------------+
```

## `#!sql DESCRIBE ... ENSEMBLE` Statement

### Description

The `DESCRIBE mindsdb.[predictor_name].ensemble` statement displays the parameters used to select the best candidate model.

### Syntax

Here is the syntax:

```sql
DESCRIBE mindsdb.[predictor_name].ensemble;
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

| Name                  | Description                                                                                |
| --------------------- | ------------------------------------------------------------------------------------------ |
| `[predictor_name]`    | Name of the model to be described.                                                         |
| `ensemble`            | Object of the JSON type describing the parameters used to select the best candidate model. |

### Example

Let's look at an example using the `home_rentals_model` model.

```sql
DESCRIBE mindsdb.home_rentals_model.ensemble;
```

On execution, we get:

```sql
+----------------------------------------------------------------------+
| ensemble                                                             |
+----------------------------------------------------------------------+
| {                                                                    |
| "encoders": {                                                        |
|   "rental_price": {                                                  |
|     "module": "NumericEncoder",                                      |
|     "args": {                                                        |
|       "is_target": "True",                                           |
|       "positive_domain": "$statistical_analysis.positive_domain"     |
|     }                                                                |
|   },                                                                 |
|   "number_of_rooms": {                                               |
|     "module": "OneHotEncoder",                                       |
|     "args": {}                                                       |
|   },                                                                 |
|   "number_of_bathrooms": {                                           |
|     "module": "BinaryEncoder",                                       |
|     "args": {}                                                       |
|   },                                                                 |
|   "sqft": {                                                          |
|     "module": "NumericEncoder",                                      |
|     "args": {}                                                       |
|   },                                                                 |
|   "location": {                                                      |
|     "module": "OneHotEncoder",                                       |
|     "args": {}                                                       |
|   },                                                                 |
|   "days_on_market": {                                                |
|     "module": "NumericEncoder",                                      |
|     "args": {}                                                       |
|   },                                                                 |
|   "neighborhood": {                                                  |
|     "module": "OneHotEncoder",                                       |
|     "args": {}                                                       |
|   }                                                                  |
| },                                                                   |
| "dtype_dict": {                                                      |
|   "number_of_rooms": "categorical",                                  |
|   "number_of_bathrooms": "binary",                                   |
|   "sqft": "integer",                                                 |
|   "location": "categorical",                                         |
|   "days_on_market": "integer",                                       |
|   "neighborhood": "categorical",                                     |
|   "rental_price": "integer"                                          |
| },                                                                   |
| "dependency_dict": {},                                               |
| "model": {                                                           |
|   "module": "BestOf",                                                |
|   "args": {                                                          |
|     "submodels": [                                                   |
|       {                                                              |
|         "module": "Neural",                                          |
|         "args": {                                                    |
|           "fit_on_dev": true,                                        |
|           "stop_after": "$problem_definition.seconds_per_mixer",     |
|           "search_hyperparameters": true                             |
|         }                                                            |
|       },                                                             |
|       {                                                              |
|         "module": "LightGBM",                                        |
|         "args": {                                                    |
|           "stop_after": "$problem_definition.seconds_per_mixer",     |
|           "fit_on_dev": true                                         |
|         }                                                            |
|       },                                                             |
|       {                                                              |
|         "module": "Regression",                                      |
|         "args": {                                                    |
|           "stop_after": "$problem_definition.seconds_per_mixer"      |
|         }                                                            |
|       }                                                              |
|     ],                                                               |
|     "args": "$pred_args",                                            |
|     "accuracy_functions": "$accuracy_functions",                     |
|     "ts_analysis": null                                              |
|   }                                                                  |
| },                                                                   |
| "problem_definition": {                                              |
|   "target": "rental_price",                                          |
|   "pct_invalid": 2,                                                  |
|   "unbias_target": true,                                             |
|   "seconds_per_mixer": 57024.0,                                      |
|   "seconds_per_encoder": null,                                       |
|   "expected_additional_time": 8.687719106674194,                     |
|   "time_aim": 259200,                                                |
|   "target_weights": null,                                            |
|   "positive_domain": false,                                          |
|   "timeseries_settings": {                                           |
|     "is_timeseries": false,                                          |
|     "order_by": null,                                                |
|     "window": null,                                                  |
|     "group_by": null,                                                |
|     "use_previous_target": true,                                     |
|     "horizon": null,                                                 |
|     "historical_columns": null,                                      |
|     "target_type": "",                                               |
|     "allow_incomplete_history": true,                                |
|     "eval_cold_start": true,                                         |
|     "interval_periods": []                                           |
|   },                                                                 |
|   "anomaly_detection": false,                                        |
|   "use_default_analysis": true,                                      |
|   "ignore_features": [],                                             |
|   "fit_on_all": true,                                                |
|   "strict_mode": true,                                               |
|   "seed_nr": 420                                                     |
| },                                                                   |
| "identifiers": {},                                                   |
| "accuracy_functions": [                                              |
|   "r2_score"                                                         |
| ]                                                                    |
|}                                                                     |
+----------------------------------------------------------------------+
```

!!! TIP "Need More Info?"
    If you need more information on how to `#!sql DESCRIBE` your model or understand the results, feel free to ask us on the [community Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

# `#!sql DESCRIBE` statement

## Description

The `DESCRIBE` statement is used to display the attributes of an existing model.

## DESCRIBE FEATURES

The `DESCRIBE mindsdb.[name_of_your_predictor].features` statement is used to display the way that the model encoded the data prior to training.

### Syntax

```sql
DESCRIBE mindsdb.[name_of_your_predictor].features;
```

On execution:

```sql
+--------------+-------------+--------------+-------------+
| column       | type        | encoder      | role        |
+--------------+-------------+--------------+-------------+
| column_name  | column_type | encoder_used | column_role |
+--------------+-------------+--------------+-------------+
```

Where:

|                            | Description                                           |
| -------------------------- | ----------------------------------------------------- |
| `[name_of_your_predictor]` | Name of the model to be described                     |
| column                     | Columns used                                          |
| type                       | Type of data infered                                  |
| encoder                    | Encoder used                                          |
| role                       | Role for that column, it can be `feature` or `target` |

### Example

```sql
DESCRIBE mindsdb.home_rentals_model.features;
```

On execution:

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

## DESCRIBE MODEL

The `DESCRIBE mindsdb.[name_of_your_predictor].model` statement is used to display the performance of the candidate models.

### Syntax

```sql
DESCRIBE mindsdb.[name_of_your_predictor].model;
```

On execution:

```sql
+-----------------+-------------+---------------+----------+
| name            | performance | training_time | selected |
+-----------------+-------------+---------------+----------+
| candidate_model | performace  | training_time | selected |
+-----------------+-------------+---------------+----------+
```

Where:

|                            | Description                                            |
| -------------------------- | ------------------------------------------------------ |
| `[name_of_your_predictor]` | Name of the model to be described                      |
| name                       | Name of the candidate_model                            |
| performance                | Accuracy From 0 - 1 depending on the type of the model |
| training_time              | Time elapsed for the model training to be completed    |
| selected                   | `1` for the best performing model `0` for the rest     |

### Example

```sql
DESCRIBE mindsdb.home_rentals_model.model;
```

On execution:

```sql
+------------+--------------------+----------------------+----------+
| name       | performance        | training_time        | selected |
+------------+--------------------+----------------------+----------+
| Neural     | 0.9861694189913056 | 3.1538941860198975   | 0        |
| LightGBM   | 0.9991920992432087 | 15.671080827713013   | 1        |
| Regression | 0.9983390488042778 | 0.016761064529418945 | 0        |
+------------+--------------------+----------------------+----------+
```

## DESCRIBE MODEL

### Syntax

```sql
DESCRIBE mindsdb.[name_of_your_predictor].ensemble;
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

### Example

```sql 
DESCRIBE mindsdb.home_rentals_model.ensemble;
```

On execution:

```sql
+----------------------------------------------------------------------+
| ensemble                                                             |
+----------------------------------------------------------------------+
| {
  "encoders": {
    "rental_price": {
      "module": "NumericEncoder",
      "args": {
        "is_target": "True",
        "positive_domain": "$statistical_analysis.positive_domain"
      }
    },
    "number_of_rooms": {
      "module": "OneHotEncoder",
      "args": {}
    },
    "number_of_bathrooms": {
      "module": "BinaryEncoder",
      "args": {}
    },
    "sqft": {
      "module": "NumericEncoder",
      "args": {}
    },
    "location": {
      "module": "OneHotEncoder",
      "args": {}
    },
    "days_on_market": {
      "module": "NumericEncoder",
      "args": {}
    },
    "neighborhood": {
      "module": "OneHotEncoder",
      "args": {}
    }
  },
  "dtype_dict": {
    "number_of_rooms": "categorical",
    "number_of_bathrooms": "binary",
    "sqft": "integer",
    "location": "categorical",
    "days_on_market": "integer",
    "neighborhood": "categorical",
    "rental_price": "integer"
  },
  "dependency_dict": {},
  "model": {
    "module": "BestOf",
    "args": {
      "submodels": [
        {
          "module": "Neural",
          "args": {
            "fit_on_dev": true,
            "stop_after": "$problem_definition.seconds_per_mixer",
            "search_hyperparameters": true
          }
        },
        {
          "module": "LightGBM",
          "args": {
            "stop_after": "$problem_definition.seconds_per_mixer",
            "fit_on_dev": true
          }
        },
        {
          "module": "Regression",
          "args": {
            "stop_after": "$problem_definition.seconds_per_mixer"
          }
        }
      ],
      "args": "$pred_args",
      "accuracy_functions": "$accuracy_functions",
      "ts_analysis": null
    }
  },
  "problem_definition": {
    "target": "rental_price",
    "pct_invalid": 2,
    "unbias_target": true,
    "seconds_per_mixer": 57024.0,
    "seconds_per_encoder": null,
    "expected_additional_time": 8.687719106674194,
    "time_aim": 259200,
    "target_weights": null,
    "positive_domain": false,
    "timeseries_settings": {
      "is_timeseries": false,
      "order_by": null,
      "window": null,
      "group_by": null,
      "use_previous_target": true,
      "horizon": null,
      "historical_columns": null,
      "target_type": "",
      "allow_incomplete_history": true,
      "eval_cold_start": true,
      "interval_periods": []
    },
    "anomaly_detection": false,
    "use_default_analysis": true,
    "ignore_features": [],
    "fit_on_all": true,
    "strict_mode": true,
    "seed_nr": 420
  },
  "identifiers": {},
  "accuracy_functions": [
    "r2_score"
  ]
}                                                                      |
+----------------------------------------------------------------------+
```

!!! TIP "Unsure what it all means?" 
    If you're unsure on how to `#!sql DESCRIBE` your model or understand the results feel free to ask us how to do it on the community [Slack workspace](https://join.slack.com/t/mindsdbcommunity/shared_invite/zt-o8mrmx3l-5ai~5H66s6wlxFfBMVI6wQ).

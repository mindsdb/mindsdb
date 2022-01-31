---
id: timeseries-mindsdb
title: Handling Timeseries Data
---

## Handling Timeseries Data

### Timeseries interface

A timeseries is a problem where rows are related to each other in a sequential way, such that the prediction of the value in the present row should take into account a number of previous rows.

To build a timeseries model you need to pass `timeseries_settings` dictionary to `learn`

```
timeseries_settings = {
  order_by: List<String>              | Mandatory
  window: Int                         | Mandatory
  group_by: List<String>              | Optional (default: [])
  horizon: Int                 | Optional (default: 1)
  use_previous_target: Bool           | Optional (default: True)
  historical_columns: List<String>    | Optional (default: [])
}
```

Let's go through these settings one by one:

* order_by - The columns based on which the data should be ordered
* group_by - The columns based on which to group multiple unrelated entities present in your timeseries data. For example, let's say your data consists of sequential readings from 3x sensors. Treating the problem as a timeseries makes sense for individual sensors, so you would specify: `group_by=['sensor_id']`
* horizon - The number of points in the future that predictions should be made for, defaults to `1`. Once trained, the model will be able to predict up to this many points into the future.
* use_previous_target - Use the previous values of the target column[s] for making predictions. Defaults to `True`.
* window - The number of rows to "look back" into when making a prediction, after the rows are ordered by the order_by column and split into groups.
* historical_columns - The temporal dynamics of these columns will be used as additional context to train the time series encoder of the predictor. Note that non-historical columns will also be used to forecast, though without considering their change through time [Status: Experimental].

### Code example

```python
import mindsdb_native

mdb = mindsdb_native.Predictor(name='assembly_machines_model')
mdb.learn(
    from_data='assembly_machines_historical_data.tsv',
    to_predict='failure',
    timeseries_settings = {
      'order_by': ['timestamp'], # Order the observations by timestamp
      'group_by': ['machine_id'], # The ordering should be done on a per-machine basis, rather than for every single row
      'horizon': 3, # Predict failures for the timestamp given and for 2 more timesteps in the future
      'use_previous_target': True, # Use the previous values in the target column (`failure`), since when the last failure happened could be a relevant data-point for our prediction.
      'window': 20, # Consider the previous 20 rows for every single row our model is trying to predict
      'historical_columns': ['sensor_activity'] # Mark `sensor_activity` column as historical, to use its temporal dynamics as additional context
    }
)

results = mdb.predict(when_data='new_assembly_machines_data.tsv')
```

### Historical data
When making timeseries predictions, it's important to provide mindsdb with the context for those predictions, i.e. with the previous rows that came before the one you are trying to predict for.

Say your columns are: `date, nr_customers, store`.
You order by `date`, group by `store` and need to predict `nr_customers`. You set `window=3`.

You train your model and then want to make a prediction using a csv with the following content:
```
date,       nr_customers, store
2020-10-06, unknown     , A1
```

This prediction will be less than ideal, since mindsdb doesn't know how many customers came to the store on `2020-10-05` or `2020-10-04`, which is probably the main insight the trained model is using to make predictions. So instead you need to pass a file with the following content:

```
date,       nr_customers, store
2020-10-04, 55          , A1
2020-10-05, 123         , A1
2020-10-06, None        , A1
```

Note that mindsdb will generate a prediction for every row here (even if the target value `nr_customers` already exists), but you only care about the prediction for the last row, the previous 2 are there to provide historical context.

Also note that, if you window was, say, equal to `5`, you would have had to provide 4 more rows instead of 2 more.
Also note that, if you were to give the file:

```
date,       nr_customers, store
2020-10-04, 55          , B11
2020-10-05, 123         , A2
2020-10-06, None        , A1
```

This wouldn't count as historical context, since you are grouping by the `store` column, so only rows where `store` is `A1` will be relevant historical context for predicting a row where the `store == A1`

## Experimental features

Some features are still very experimental and have many blindspots, so if you're interested in using them please contact us so we can help and get your feedback on how to improve.

### Database integration

When you train MindsDB from a database, it can auto-generate a query to select historical context, based on the query you used to source your training data.

This can be enabled by passing `advanced_args={'use_database_history': True}` to the `predict` call (or to the `SELECT` call if operating from within a database). 

### Anomaly detection

We support anomaly detection for numerical time series. Our approach tags a data point as anomalous if it falls outside of the predicted range.

Normally, MindsDB will try to generate bounds that are tight enough to be informative about your predictions. However, as this feature is  unsupervised (because it does not need labeled anomalies in the data), we offer two parameters to tune it for any specific use case:

`anomaly_error_rate` (Float, `0 < x < 1`): If smaller, it leads to wider bounds, thus less sensitivity to anomalies. If bigger, it leads to narrower bounds and increased sensitivity. By default, MindsDB will try to find an appropriate value.

`anomaly_cooldown` (Int): period during which no anomalies will be flagged, triggered after an initial anomaly is detected. Copes with potential predictor accuracy issues while it adjusts to the anomalous event.

These parameters can be passed in the `advanced_args` dictionary when calling `predict`, and do not require predictor re-training.

### Database example (from SQL)
-- Pending, feel free to contribute some or ask us directly about this feature

### Database example (from code)
-- Pending, feel free to contribute some or ask us directly about this feature

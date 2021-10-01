# Train a model from ClickHouse database

![MindsDB-ClickHouse](/assets/databases/mdb-clickhouse.png)

### Train new model

To train a new model, you will need to `INSERT` a new record inside the mindsdb.predictors table.

!!! question "How to create the mindsDB database and tables"
    Note that after connecting [MindsDB and ClickHouse](/datasources/clickhouse/#clickhouse-client), on start, the MindsDB server will automatically create the mindsDB database and add the predictors table.

The `INSERT` query for training a new model is quite simple, e.g.:

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query, training_options)
VALUES('model_name', 'target_variable', 'SELECT * FROM table_name', '{"additional_training_params:value"}');
```
The values provided in the `INSERT` query are:

* name (string) -- The name of the model.
* predict (string) --  The feature you want to predict. To predict multiple features, include a comma separated string, e.g. 'feature1, feature2'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of the parameters check the [PredictorInterface](/PredictorInterface/#learn).

![Train model from ClickHouse client](/assets/predictors/clickhouse-insert.gif)

### Train new model example

The following example shows you how to train a new model from the ClickHouse client. The table used for training the model is the [Air Pollution in Seoul](https://www.kaggle.com/bappekim/air-pollution-in-seoul) timeseries dataset.

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query,training_options)
VALUES('airq_model', 'SO2', 'SELECT * FROM default.pollution_measurement', '{"timeseries_settings":{"order_by": ["Measurement date"], "window":20}}');
```

This `INSERT` query will train a new model called `pollution_measurement` that predicts the passenger `SO2` value.
Since, this is a timeseries dataset, the `timeseries_settings` will order the data by the `Measurement date` column and will set the window for rows to "look back" when making a prediction.

### Model training status

To check that the training finished successfully, you can `SELECT` from mindsdb.predictors table and get the training status, e.g.:

```sql
SELECT * FROM mindsdb.predictors WHERE name='<model_name>';
```

![Training model status](/assets/predictors/clickhouse-status.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully trained a new model from a ClickHouse database. The next step is to get predictions by [querying the model](/model/query/clickhouse/).


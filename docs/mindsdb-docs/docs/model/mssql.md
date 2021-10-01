# Train a model from Microsoft SQL Server



### Train new model

To train a new model, you will need to `INSERT` a new record inside the mindsdb.predictors table.

!!! question "How to create the mindsb.predictors table"
    Note that after connecting the [MindsDB and Microsoft SQL server](/datasources/mssql/#mssql-client), on
    start, the MindsDB server will automatically create the mindsdb database and add the predictors table.

!!! info "Prerequisite"
    Don't forget to install the prerequisites as explained in [connect your data section](/datasources/mssql/#prerequisite).

There are both options for training the model by using the `openquery` or `exec` statement. The `INSERT` query for training new model using `exec` statement:

```sql
exec ('INSERT INTO mindsdb.predictors (name, predict, select_data_query) VALUES (''model_name'', ''target_variable'', ''SELECT * FROM table_name'')') AT mindsdb;
```
Or, `openquery`:

```sql
INSERT openquery(mindsdb,'SELECT name, predict, select_data_query FROM mindsdb.predictors WHERE 1=0') VALUES ('model_name','target_variable','SELECT * FROM table_name');
```

The values provided in the `INSERT` query are:

* name (string) -- The name of the model.
* predict (string) --  The feature you want to predict. To predict multiple features, include a comma separated string, e.g. 'feature1,feature2'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of parameters, check the [PredictorInterface](/PredictorInterface/#learn).

![Train model from mssql client](/assets/predictors/mssql-insert.gif)

### Train new model example

The following example shows you how to train a new model from a mssql client. The table used for training the model is the [Medical insurance](https://www.kaggle.com/mirichoi0218/insurance) dataset.

```sql
exec ('INSERT INTO mindsdb.predictors (name, predict, select_data_query) 
VALUES ("insurance_model", "charges", "SELECT * FROM mindsdb_test.dbo.insurance")') 
AT mindsdb;
```

This `INSERT` query will train a new model called `insurance_model` that predicts the `charges` value.

#### Model training status

To check that the training finished successfully, you can `SELECT` from the mindsdb.predictors table and get the training status, e.g.:

```sql
exec ('SELECT * FROM mindsdb.predictors') AT mindsdb;
```

>Note: `mindsdb` is the name of the api['mysql]['database'] key from config.json. The default name is `mindsdb`.


![Training model status](/assets/predictors/mssql-status.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully trained a new model from a Microsoft SQL Server. The next step is to get predictions by [querying the model](/model/query/mssql).


### Train time series model example


The table used for training the model is the [Air Pollution in Seoul](https://www.kaggle.com/bappekim/air-pollution-in-seoul) timeseries dataset.

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



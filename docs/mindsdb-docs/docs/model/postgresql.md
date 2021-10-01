# Train a model from PostgreSQL database

![MindsDB-MySQL](/assets/databases/mdb-postgres.png)

### Train new model

To train a new model, you will need to `INSERT` a new record inside the mindsdb.predictors table.

!!! question "How to create the mindsb schema and tables"
    Note that after connecting [MindsDB and PostgreSQL](/datasources/postgresql/#psql-client), on start, the MindsDB server will automatically create the mindsDB schema and add the predictors table.


!!! info "Prerequisite"
    Don't forget to install the MySQL foreign data wrapper as explained in [connect your data section](/datasources/postgresql/#prerequisite).

The `INSERT` query for training a new model is quite simple, e.g.:

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query, training_options)
VALUES ('model_name', 'target_variable', 'SELECT * FROM table_name', '{"additional_training_params:value"}');
```
The values provided in the `INSERT` query are:

* name (string) -- The name of the model.
* predict (string) --  The feature you want to predict. To predict multiple features, include a comma separated string, e.g. 'feature1,feature2'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of parameters, check the [PredictorInterface](/PredictorInterface/#learn).

![Train model from psql client](/assets/predictors/postgresql-insert.gif)

!!! info "Timeseries"
    To train timeseries model, check out the [timeseries example](/model/timeseries).

### Train new model example

The following example shows you how to train a new model from a psql client. The table used for training the model is the [Airline Passenger Satisfaction](https://www.kaggle.com/teejmahal20/airline-passenger-satisfaction) dataset.

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query) VALUES('airline_survey_model', 'satisfaction', 'SELECT * FROM airline_passenger_satisfaction');
```
This `INSERT` query will train a new model called `airline_survey_model` that predicts the passenger `satisfaction` value.

### Model training status

To check that the training finished successfully, you can `SELECT` from the mindsdb.predictors table and get the training status, e.g.:

```sql
SELECT * FROM mindsdb.predictors WHERE name='<model_name>';
```

![Training model status](/assets/predictors/postgresql-status.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully trained a new model from  PostgreSQL database. The next step is to get predictions by [querying the model](/model/query/postgresql/).

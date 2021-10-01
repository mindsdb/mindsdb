# How to train a model from MariaDB?

![MindsDB-MariaDB](/assets/databases/mdb-maria.png)

### How to train a new model

To train a new model, you will need to `INSERT` a new record inside the mindsdb.predictors table.

!!! question "How to create the mindsb.predictors table"
    Note that after connecting [MindsDB and MariaDB](/datasources/mariadb/#mysql-client), on start, the MindsDB server will automatically create the mindsdb database and add the predictors table.

!!! info "Prerequisite"
    Don't forget to enable CONNECT Storage Engine as explained in [connect your data section](/datasources/mariadb/#prerequisite).

The `INSERT` query for training new model is quite simple, e.g.:

```sql
INSERT INTO mindsdb.predictors(name, predict, select_data_query, training_options)
VALUES('model_name', 'target_variable', 'SELECT * FROM table_name');
```
The values provided in the `INSERT` query are:

* name (string) -- The name of the model.
* predict (string) --  The feature you want to predict. To predict multiple features, include a comma separated string, e.g. 'feature1,feature2'.
* select_data_query (string) -- The SELECT query that will ingest the data to train the model.
* training_options (JSON as comma separated string) -- optional value that contains additional training parameters. For a full list of parameters, check the [PredictorInterface](/PredictorInterface/#learn).

![Train model from MariaDB client](/assets/predictors/mariadb-insert.gif)

!!! info "Timeseries"
    To train timeseries model, check out the [timeseries example](/model/timeseries).

### Train new model example

The following example shows you how to train a new model from the MariaDB client. The table used for training the model is the [Used cars](https://www.kaggle.com/adityadesai13/used-car-dataset-ford-and-mercedes) dataset.

```sql
INSERT INTO
  mindsdb.predictors(name, predict, select_data_query)
VALUES
  ('used_cars_model', 'price', 'SELECT * FROM data.used_cars_data');
```

The `INSERT` query will train a new model called `used_cars_model` that predicts the cars `price` value.

### How to check model training status?

To check that the training finished successfully, you can `SELECT` from the mindsdb.predictors table and get the training status, e.g.:

```sql
SELECT * FROM mindsdb.predictors WHERE name='<model_name>';
```

![Training model status](/assets/predictors/mariadb-status.gif)

!!! Success "That's it :tada: :trophy:  :computer:"
    You have successfully trained a new model from the MariaDB database. The next step is to get predictions by [querying the model](/model/query/mariadb).

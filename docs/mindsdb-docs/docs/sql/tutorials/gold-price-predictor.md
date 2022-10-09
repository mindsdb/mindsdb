# Predicting Gold Price with MindsDB

## Introduction

In this tutorial, we'll create and train a machine learning model, or as we call it, `gold_pred_model`. By querying the model, we'll predict the gold prices through time series forecasting.

Make sure you have access to a working MindsDB installation, either locally or at [MindsDB Cloud](https://cloud.mindsdb.com/).

If you want to learn how to set up your account at MindsDB Cloud, follow [this guide](https://docs.mindsdb.com/setup/cloud/). Another way is to set up MindsDB locally using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [Python](https://docs.mindsdb.com/setup/self-hosted/pip/source/).

Let's get started.
## Data Setup
### Connecting Database
Link your chosen database to MindsDB using CREATE DATABASE query
```sql
CREATE DATABASE example_db
WITH ENGINE = "postgres",
PARAMETERS = {
    "user": "demo_user",
    "password": "demo_password",
    "host": "3.220.66.106",
    "port": "5432",
    "database": "demo"
    };
```
**or**

For this tutorial we are using kaggle's the *Learn Time Series Forecasting From Gold Price* data. 

You can download [the `CSV` data file here](https://www.kaggle.com/code/anayad/learning-time-series-forecasting) (we use the *gold_price_data.csv* file) and upload it via [MindsDB SQL Editor](/connect/mindsdb_editor/).

### Understanding the Data

We use the gold price dataset, where we have one column as *date column* and another represents *value* of gold at that time.
Run following SQL query to have an insight:

=== "Connecting as a file"
```sql
SELECT * 
FROM files.gold_price_data 
LIMIT 5;
```

**or**

=== "Connecting as a database"
```sql
SELECT * 
FROM example_db.demo_data.gold_price_data 
LIMIT 5;
```
Above query should return 5 rows from the *gold_price_data* database.
Below is the sample data stored 
```sql
+-----------+-----+
| Date     |Value |
+-----------+-----+
|1970-01-01|35.2  |
|1970-04-01|35.1  |
|1970-07-01|35.4  |
|1970-10-01|36.2  |
|1971-01-01|37.4  |
+-----------+-----+
```

Where:

```sql
| Column  | Description                                                           | Data Type | Usage   |
| --------| --------------------------------------------------------------------------------------------|
| `Date`  | A date column (or other time step column)                             | `integer` | Feature |
| `Value` | A column that represents a metric or a value that you want to forecast| `integer` | Label   |

```

As dataset indicates we are going for **time series forecasting** for price prediction 

**What exactly is time series forecasting?**

Rather than using outliers or categories to make predictions, time series forecasting describes predictions that are made with time-stamped historical data.

## Training a Predictor

Let's create and train the machine learning model. For that, we use the [`#!sql CREATE PREDICTOR`](/sql/create/predictor) statement and specify the input columns used to train `#!sql FROM` (features) and what we want to `#!sql PREDICT` (labels).
```sql
CREATE PREDICTOR mindsdb.predictor_name (Use the name you want)

FROM databse_name                       (Use the Database Name)

(SELECT * FROM table_name)              (Use the training table name)

PREDICT target_parameter;               (Use the parameter to predict)
```
For out tutorial we gonna run below query:
```sql
CREATE PREDICTOR 
  mindsdb.gold_pred_model
FROM files
  (SELECT * FROM gold_price_data)
PREDICT Value;
```
This query should return successful status without any errors in the Result Viewer.

## Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of the predictor by using this SQL command:

```sql
SELECT status
FROM mindsdb.predictors
WHERE name='gold_pred_model';
```
If we run it right after creating a predictor, we get this output:

```sql
+------------+
| status     |
+------------+
| generating |
+------------+
```

A bit later, this is the output:

```sql
+----------+
| status   |
+----------+
| training |
+----------+
```

And at last, this should be the output:

```sql
+----------+
| status   |
+----------+
| complete |
+----------+
```

Now, if the status of our predictor says `complete`, we can start making predictions!

## Describing the Model
Now, that we have created a model and already trained it to do the prediction for us, let us dive deep into it and find out the minute details about the model.

MindsDB provides the `DESCRIBE` statement to explain the different attributes of the available models. We can describe a predictor in the following ways.

By Features
```sql
DESCRIBE mindsdb.model_name.features;
```

By Model
```sql
DESCRIBE mindsdb.model_name.model;
```

By Model Ensemble
```sql
DESCRIBE mindsdb.model_name.Model;
```

Here let's try Describe by Ensemble so we gonna run following query for that 
```sql
DESCRIBE gold_pred_model.ensemble;
```
This statement returns a JSON-type object explaining the attributes used to select the best candidate model to do the target prediction.

## Making Predictions

We are now ready to do our first prediction based on the features using the `SELECT` statement. We can predict the price of gold during specific time as run the query.
```sql
SELECT target_column_name                  (Name of the Target Column - Value)

FROM mindsdb.model_name                    (Model Name - gold_pred_model)

WHERE feature_name="value_of_the_feature"; (Feature Name - Date)
```
## Conclusion
We have finally predicted the gold prices as we wanted by using a predictor model powered by MindsDB. As all of you would have noticed, it is so simple to get this task done. All you need to do is connect your database, import a dataset, and run a few simple queries to get the prediction model up and running.

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

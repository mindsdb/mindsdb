# Forecasting Quarterly House Sales with MindsDB

## Introduction

In this tutorial, we'll create and train a machine learning model, or as we call it, an `AI Table` or a `predictor`. By querying the model, we'll predict the sales forecasts for houses using a multivariate time series strategy.

Make sure you have access to a working MindsDB installation, either locally or at [MindsDB Cloud](https://cloud.mindsdb.com/).

If you want to learn how to set up your account at MindsDB Cloud, follow [this guide](https://docs.mindsdb.com/setup/cloud/). Another way is to set up MindsDB locally using [Docker](https://docs.mindsdb.com/setup/self-hosted/docker/) or [Python](https://docs.mindsdb.com/setup/self-hosted/pip/source/).

Let's get started.

## Data Setup

### Connecting the Data

There are a couple of ways you can get the data to follow through with this tutorial.

=== "Connecting as a database"

    You can connect to a demo database that we've prepared for you. It contains the data used throughout this tutorial (the `#!sql example_db.demo_data.house_sales` table).

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

    Now you can run queries directly on the demo database. Let's preview the data that we'll use to train our predictor.

    ```sql
    SELECT * 
    FROM example_db.demo_data.house_sales 
    LIMIT 10;
    ```

=== "Connecting as a file"

    The dataset we use in this tutorial is the pre-processed version of the *House Property Sales* data. You can download [the `CSV` data file here](https://www.kaggle.com/datasets/htagholdings/property-sales) (we use the *ma_lga_12345.csv* file) and upload it via [MindsDB SQL Editor](/connect/mindsdb_editor/).

    Follow [this guide](/sql/create/file/) to find out how to upload a file to MindsDB.

    Now you can run queries directly on the file as if it were a table. Let's preview the data that we'll use to train our predictor.

    ```sql
    SELECT *
    FROM files.house_sales
    LIMIT 10;
    ```

!!! Warning "Pay Attention to the Queries"
    From now on, we'll use the `#!sql files.house_sales` table. Make sure you replace it with `example_db.demo_data.house_sales` if you connect the data as a database.

### Understanding the Data

We use the house sales dataset, where each row is one house or one unit, to predict the `MA` column values. It tracks quarterly moving averages of house sales aggregated by real estate type and number of bedrooms in each listing. These quarterly moving averages will be predicted in the following sections of this tutorial.

Below is the sample data stored in the `#!sql files.house_sales` table.

```sql
+----------+------+-----+--------+
|saledate  |MA    |type |bedrooms|
+----------+------+-----+--------+
|30/09/2007|441854|house|2       |
|31/12/2007|441854|house|2       |
|31/03/2008|441854|house|2       |
|30/06/2016|430880|unit |2       |
|30/09/2016|430654|unit |2       |
+----------+------+-----+--------+
```

Where:

| Column                | Description                                                                                  | Data Type           | Usage   |
| --------------------- | -------------------------------------------------------------------------------------------- | ------------------- | ------- |
| `saledate`            | The date of sale.                                                                            | `date`              | Feature |
| `MA`                  | Moving average of the historical median price of the house or unit.                          | `integer`           | Label   |
| `type`                | Type of property (`house` or `unit`).                                                        | `character varying` | Feature |
| `bedrooms`            | Number of bedrooms.                                                                          | `integer`           | Feature |

!!!Info "Labels and Features"
    A **label** is a column whose values will be predicted (the y variable in simple linear regression).<br/>
    A **feature** is a column used to train the model (the x variable in simple linear regression).

## Training a Predictor

Let's create and train the machine learning model. For that, we use the [`#!sql CREATE PREDICTOR`](/sql/create/predictor) statement and specify the input columns used to train `#!sql FROM` (features) and what we want to `#!sql PREDICT` (labels).

```sql
CREATE PREDICTOR mindsdb.house_sales_predictor
FROM files
  (SELECT * FROM house_sales)
PREDICT MA
ORDER BY saledate
GROUP BY bedrooms, type
-- the target column to be predicted stores one row per quarter
WINDOW 8      -- using data from the last two years to make forecasts (last 8 rows)
HORIZON 4;    -- making forecasts for the next year (next 4 rows)
```

We use all of the columns as features, except for the `MA` column, whose values will be predicted.

MindsDB makes it simple so that we don't need to repeat the predictor creation process for every group, that is, for every distinct number of bedrooms or for every distinct type of a real estate. Instead, we just group by both the `bedrooms` and `type` columns and the predictor learns from all series and enables forecasts for all of them!

## Status of a Predictor

A predictor may take a couple of minutes for the training to complete. You can monitor the status of the predictor by using this SQL command:

```sql
SELECT status
FROM mindsdb.predictors
WHERE name='house_sales_predictor';
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

## Making Predictions

### Making a Single Prediction

You can make predictions by querying the predictor as if it were a table. The [`SELECT`](/sql/api/select/) statement lets you make predictions for the label based on the chosen features for a given time period. Usually, you want to know what happens right after the latest training data point that was fed. We have a special keyword for that, the `LATEST` keyword.

```sql
SELECT m.saledate AS date, m.MA AS forecast
FROM mindsdb.house_sales_predictor AS m 
JOIN files.house_sales AS t
WHERE t.saledate > LATEST 
AND t.type = 'house' 
AND t.bedrooms = 2
LIMIT 4;
```

On execution, we get:

```sql
TODO
```

Now, try changing the `type` column value to *unit*, or the `bedrooms` column value to any number between 1 to 5, and check how the forecasts vary. This is because MindsDB recognizes each grouping as being its own different time series.

### Making Batch Predictions

Also, you can make bulk predictions by joining a data table with your predictor using [`#!sql JOIN`](/sql/api/join).

```sql
TODO
```

On execution, we get:

```sql
TODO
```

## What's Next?

Have fun while trying it out yourself!

* Bookmark [MindsDB repository on GitHub](https://github.com/mindsdb/mindsdb).
* Sign up for a free [MindsDB account](https://cloud.mindsdb.com/register).
* Engage with the MindsDB community on [Slack](https://mindsdb.com/joincommunity) or [GitHub](https://github.com/mindsdb/mindsdb/discussions) to ask questions and share your ideas and thoughts.

If this tutorial was helpful, please give us a GitHub star [here](https://github.com/mindsdb/mindsdb).
